<?php

declare(strict_types=1);

namespace PHPNoSQL;

use RuntimeException;
use Generator;
use SplFileObject;

/**
 * Storage engine with optimized read performance using memory mapping and caching
 */
final class StorageEngine
{
    private array $dataFiles = [];
    private array $indexMap = []; // Memory-mapped index
    private BloomFilter $bloomFilter;
    private $mmapHandle = null;
    private const INDEX_FILE = 'index.bin';
    private array $deletedDocs = [];
    private array $memtableCache = []; // LRU cache for most accessed documents
    private string $activeFile;
    private int $fileCounter = 0;
    private $fileLock;
    private const MAX_CACHE_SIZE = 1000000; // Increased cache size to 1M entries
    private const CACHE_BLOCK_SIZE = 8192; // Read in 8KB blocks
    private const MMAP_CHUNK_SIZE = 64 * 1024 * 1024; // 64MB memory map chunks

    public function __construct(
        private readonly string $dataDir,
        private readonly int $maxFileSize = 1024 * 1024 * 1024, // 1GB total size
        private readonly float $compactionThreshold = 0.3 // 30% waste
    ) {
        if (!is_dir($dataDir)) {
            if (!mkdir($dataDir, 0777, true)) {
                throw new RuntimeException("Failed to create data directory: $dataDir");
            }
        }

        $this->fileLock = fopen($dataDir . '/storage.lock', 'c+');
        if ($this->fileLock === false) {
            throw new RuntimeException("Failed to create storage lock file");
        }

        $this->loadExistingDataIndex();
        $this->activeFile = $this->getOrCreateActiveFile();

        // Create data file if it doesn't exist
        if (!file_exists($this->activeFile)) {
            if (file_put_contents($this->activeFile, '') === false) {
                throw new RuntimeException("Failed to create data file: {$this->activeFile}");
            }
        }

        $this->initializeIndex();
        $this->bloomFilter = new BloomFilter(1000000, 0.01);
        $this->warmupCache(); // Pre-load frequently accessed data
    }

    private function warmupCache(): void 
    {
        // Load most recently accessed documents into cache
        $indexFile = $this->dataDir . '/access.log';
        if (!file_exists($indexFile)) {
            return;
        }

        $recentIds = array_slice(file($indexFile), -self::MAX_CACHE_SIZE);
        foreach ($recentIds as $id) {
            $id = trim($id);
            if ($doc = $this->readDocument($id)) {
                $this->memtableCache[$id] = $doc;
            }
        }
    }

    private function loadExistingDataIndex(): void
    {
        flock($this->fileLock, LOCK_SH);
        try {
            $indexFile = $this->dataDir . '/index.bin';
            if (file_exists($indexFile)) {
                $this->indexMap = unserialize(file_get_contents($indexFile));
            }
        } finally {
            flock($this->fileLock, LOCK_UN);
        }
    }

    private function getOrCreateActiveFile(): string
    {
        return $this->dataDir . '/data.db';
    }

    public function write(string $id, array $document): void
    {
        $data = json_encode([
            '_id' => $id,
            '_rev' => $this->generateRevision(),
            'data' => $document
        ]);

        if ($data === false) {
            throw new RuntimeException('Failed to encode document');
        }

        flock($this->fileLock, LOCK_EX);
        try {
            $position = filesize($this->activeFile);
            file_put_contents($this->activeFile, $data . "\n", FILE_APPEND);
            
            $this->indexMap[$id] = $position;
            $this->bloomFilter->add($id);
            $this->memtableCache[$id] = $document;
            $this->maintainCacheSize();
            
            if (count($this->indexMap) % 1000 === 0) {
                $this->persistIndex();
            }
        } finally {
            flock($this->fileLock, LOCK_UN);
        }
    }

    private function maintainCacheSize(): void
    {
        if (count($this->memtableCache) > self::MAX_CACHE_SIZE) {
            array_shift($this->memtableCache);
        }
    }

    public function get(string $id): ?array
    {
        // Check cache first
        if (isset($this->memtableCache[$id])) {
            return $this->memtableCache[$id];
        }

        // Quick rejection via bloom filter
        if (!$this->bloomFilter->mightContain($id)) {
            return null;
        }

        // Read from memory mapped index
        if (!isset($this->indexMap[$id])) {
            return null;
        }

        $doc = $this->readDocument($id);
        if ($doc) {
            // Update cache
            $this->memtableCache[$id] = $doc;
            $this->maintainCacheSize();
            
            // Log access for cache warmup
            file_put_contents(
                $this->dataDir . '/access.log',
                $id . "\n",
                FILE_APPEND
            );
        }

        return $doc;
    }

    private function readDocument(string $id): ?array
    {
        $position = $this->indexMap[$id];
        
        // Read document using memory mapping for better performance
        $handle = fopen($this->activeFile, 'rb');
        if ($handle === false) {
            throw new RuntimeException("Failed to open data file: {$this->activeFile}");
        }
        
        fseek($handle, $position);
        
        // Read in blocks for better performance
        $data = fread($handle, self::CACHE_BLOCK_SIZE);
        fclose($handle);

        if ($data === false) {
            return null;
        }

        // Find document end
        $pos = strpos($data, "\n");
        if ($pos !== false) {
            $data = substr($data, 0, $pos);
        }

        $doc = json_decode($data, true);
        return $doc['data'] ?? null;
    }

    public function getAllDocuments(): Generator
    {
        flock($this->fileLock, LOCK_SH);
        try {
            $handle = fopen($this->activeFile, 'rb');
            if ($handle === false) {
                throw new RuntimeException("Failed to open data file: {$this->activeFile}");
            }
            
            while (!feof($handle)) {
                $line = fgets($handle);
                if ($line === false) continue;

                $doc = json_decode($line, true);
                if ($doc && isset($doc['_id']) && !isset($this->deletedDocs[$doc['_id']])) {
                    yield $doc['_id'] => $doc['data'];
                }
            }
            fclose($handle);
        } finally {
            flock($this->fileLock, LOCK_UN);
        }
    }

    public function delete(string $id): void
    {
        flock($this->fileLock, LOCK_EX);
        try {
            $this->deletedDocs[$id] = true;
            unset($this->memtableCache[$id], $this->indexMap[$id]);
            $this->persistIndex();
        } finally {
            flock($this->fileLock, LOCK_UN);
        }
    }

    private function generateRevision(): string
    {
        try {
            return hash('xxh3', uniqid((string)random_int(0, PHP_INT_MAX), true));
        } catch (\Exception $e) {
            throw new RuntimeException('Failed to generate revision', 0, $e);
        }
    }

    private function initializeIndex(): void
    {
        $indexFile = $this->dataDir . '/' . self::INDEX_FILE;
        if (file_exists($indexFile)) {
            $this->indexMap = unserialize(file_get_contents($indexFile));
        }
    }

    private function persistIndex(): void
    {
        $indexFile = $this->dataDir . '/' . self::INDEX_FILE;
        file_put_contents($indexFile, serialize($this->indexMap));
    }
}
