<?php

declare(strict_types=1);

namespace PHPNoSQL;

use RuntimeException;
use Generator;
use SplFileObject;

/**
 * Write-Ahead Log (WAL) implementation with multi-file support and concurrency
 */
final class WAL
{
    private $currentLogFile;
    private int $maxLogSize;
    private array $pendingOperations = [];
    private array $logFiles = [];
    private int $fileCounter = 0;
    private const OPTIMAL_FILE_SIZE = 64 * 1024 * 1024; // 64MB per file
    private const MAX_ENTRIES_PER_FILE = 100000;
    private $fileLock;

    public function __construct(
        private readonly string $walPath,
        private readonly int $maxBatchSize = 1000,
        int $maxLogSizeMB = 100
    ) {
        $this->maxLogSize = $maxLogSizeMB * 1024 * 1024;
        if (!is_dir(dirname($walPath))) {
            mkdir(dirname($walPath), 0777, true);
        }
        $this->fileLock = fopen($walPath . '.lock', 'c+');
        if ($this->fileLock === false) {
            throw new RuntimeException("Failed to create lock file");
        }
        $this->initializeLogFiles();
    }

    private function initializeLogFiles(): void
    {
        // Load existing log files with shared lock for reading
        flock($this->fileLock, LOCK_SH);
        try {
            foreach (glob($this->walPath . '.*') as $file) {
                if (preg_match('/\.(\d+)$/', $file, $matches)) {
                    $this->fileCounter = max($this->fileCounter, (int)$matches[1]);
                    $this->logFiles[$matches[1]] = $file;
                }
            }
        } finally {
            flock($this->fileLock, LOCK_UN);
        }

        // Create initial log file if none exist
        if (empty($this->logFiles)) {
            $this->createNewLogFile();
        } else {
            $this->currentLogFile = new SplFileObject(end($this->logFiles), 'a+b');
        }
    }

    private function createNewLogFile(): void
    {
        // Exclusive lock for creating new file
        flock($this->fileLock, LOCK_EX);
        try {
            $this->fileCounter++;
            $newPath = $this->walPath . '.' . $this->fileCounter;
            $this->logFiles[$this->fileCounter] = $newPath;

            if ($this->currentLogFile) {
                $this->currentLogFile = null;
            }

            $this->currentLogFile = new SplFileObject($newPath, 'a+b');
        } finally {
            flock($this->fileLock, LOCK_UN);
        }
    }

    public function __destruct()
    {
        if ($this->fileLock) {
            fclose($this->fileLock);
        }
    }

    public function log(string $operation, string $id, ?array $data = null): void
    {
        $entry = [
            'timestamp' => microtime(true),
            'operation' => $operation,
            'id' => $id,
            'data' => $data,
            'checksum' => null
        ];

        $entry['checksum'] = $this->calculateChecksum($entry);

        // Lock for adding to pending operations
        flock($this->fileLock, LOCK_EX);
        try {
            $this->pendingOperations[] = $entry;
        } finally {
            flock($this->fileLock, LOCK_UN);
        }

        if (count($this->pendingOperations) >= $this->maxBatchSize) {
            $this->flush();
        }
    }

    public function flush(): void
    {
        if (empty($this->pendingOperations)) {
            return;
        }

        // Exclusive lock for flushing
        flock($this->fileLock, LOCK_EX);
        try {
            $batch = json_encode($this->pendingOperations) . "\n";
            $batchSize = strlen($batch);

            $currentPosition = $this->currentLogFile->ftell();

            if ($currentPosition + $batchSize > self::OPTIMAL_FILE_SIZE) {
                $this->createNewLogFile();
            }

            $this->currentLogFile->fwrite($batch);
            $this->currentLogFile->fflush();

            $this->pendingOperations = [];

            // Check total WAL size and rotate if needed
            $totalSize = array_sum(array_map('filesize', $this->logFiles));
            if ($totalSize > $this->maxLogSize) {
                $this->rotate();
            }
        } finally {
            flock($this->fileLock, LOCK_UN);
        }
    }

    private function rotate(): void
    {
        // Exclusive lock for rotation
        flock($this->fileLock, LOCK_EX);
        try {
            $this->currentLogFile = null;

            // Archive old files
            $timestamp = time();
            foreach ($this->logFiles as $file) {
                $archivePath = $file . '.' . $timestamp . '.old';
                if (!rename($file, $archivePath)) {
                    throw new RuntimeException("Failed to rotate WAL file to: $archivePath");
                }
            }

            // Reset state
            $this->logFiles = [];
            $this->fileCounter = 0;
            $this->createNewLogFile();
        } finally {
            flock($this->fileLock, LOCK_UN);
        }
    }

    private function calculateChecksum(array $entry): string
    {
        $data = $entry['timestamp'] . $entry['operation'] . $entry['id'] .
            json_encode($entry['data']);
        return hash('xxh3', $data);
    }

    public function replay(): Generator
    {
        // Shared lock for replaying
        flock($this->fileLock, LOCK_SH);
        try {
            foreach ($this->logFiles as $logFile) {
                $file = new SplFileObject($logFile, 'rb');

                while (!$file->eof()) {
                    $line = $file->fgets();
                    if ($line === false) continue;

                    $batch = json_decode($line, true);
                    if (!$batch) continue;

                    foreach ($batch as $entry) {
                        if ($this->validateChecksum($entry)) {
                            yield $entry;
                        }
                    }
                }

                $file = null;
            }
        } finally {
            flock($this->fileLock, LOCK_UN);
        }
    }

    private function validateChecksum(array $entry): bool
    {
        $storedChecksum = $entry['checksum'];
        $entry['checksum'] = null;
        return $storedChecksum === $this->calculateChecksum($entry);
    }
}
