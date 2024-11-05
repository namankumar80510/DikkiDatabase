<?php

declare(strict_types=1);

namespace PHPNoSQL;

use RuntimeException;
use Generator;

/**
 * Main database class with batching support and concurrency
 */
final class PHPNoSQL
{
    private WAL $wal;
    private StorageEngine $storage;
    private array $batchOperations = [];
    private readonly int $maxBatchSize;
    private bool $autoCommit;
    private $dbLock;

    public function __construct(
        private readonly string $dbPath,
        int $maxBatchSize = 1000,
        bool $autoCommit = true
    ) {
        if (!is_dir($dbPath)) {
            if (!mkdir($dbPath, 0777, true)) {
                throw new RuntimeException("Failed to create database directory: $dbPath");
            }
        }

        $this->dbLock = fopen($dbPath . '/db.lock', 'c+');
        if ($this->dbLock === false) {
            throw new RuntimeException("Failed to create database lock file");
        }

        $this->maxBatchSize = $maxBatchSize;
        $this->autoCommit = $autoCommit;

        $this->wal = new WAL($dbPath . '/wal.log');
        $this->storage = new StorageEngine($dbPath . '/data');

        $this->recover();
    }

    public function put(string $id, array $document): void
    {
        if (empty($id)) {
            throw new RuntimeException('Document ID cannot be empty');
        }

        $operation = [
            'type' => 'PUT',
            'id' => $id,
            'data' => $document
        ];

        // Exclusive lock for batch operations
        flock($this->dbLock, LOCK_EX);
        try {
            $this->batchOperations[] = $operation;
            $this->wal->log('PUT', $id, $document);

            if ($this->autoCommit && count($this->batchOperations) >= $this->maxBatchSize) {
                $this->commit();
            }
        } finally {
            flock($this->dbLock, LOCK_UN);
        }
    }

    public function get(string $id): ?array
    {
        if (empty($id)) {
            throw new RuntimeException('Document ID cannot be empty');
        }
        // Shared lock for reading
        flock($this->dbLock, LOCK_SH);
        try {
            return $this->storage->get($id);
        } finally {
            flock($this->dbLock, LOCK_UN);
        }
    }

    public function getAllDocuments(): Generator
    {
        // Shared lock for reading
        flock($this->dbLock, LOCK_SH);
        try {
            yield from $this->storage->getAllDocuments();
        } finally {
            flock($this->dbLock, LOCK_UN);
        }
    }

    public function delete(string $id): void
    {
        if (empty($id)) {
            throw new RuntimeException('Document ID cannot be empty');
        }

        // Exclusive lock for deletion
        flock($this->dbLock, LOCK_EX);
        try {
            $operation = [
                'type' => 'DELETE',
                'id' => $id
            ];

            $this->batchOperations[] = $operation;
            $this->wal->log('DELETE', $id);

            if ($this->autoCommit && count($this->batchOperations) >= $this->maxBatchSize) {
                $this->commit();
            }
        } finally {
            flock($this->dbLock, LOCK_UN);
        }
    }

    public function commit(): void
    {
        if (empty($this->batchOperations)) {
            return;
        }

        // Exclusive lock for commit
        flock($this->dbLock, LOCK_EX);
        try {
            foreach ($this->batchOperations as $operation) {
                if ($operation['type'] === 'PUT') {
                    $this->storage->write($operation['id'], $operation['data']);
                } else if ($operation['type'] === 'DELETE') {
                    $this->storage->delete($operation['id']);
                }
            }

            $this->batchOperations = [];
            $this->wal->flush();
        } catch (RuntimeException $e) {
            $this->batchOperations = [];
            throw new RuntimeException('Failed to commit batch operations', 0, $e);
        } finally {
            flock($this->dbLock, LOCK_UN);
        }
    }

    public function beginBatch(): void
    {
        flock($this->dbLock, LOCK_EX);
        $this->autoCommit = false;
    }

    public function endBatch(): void
    {
        try {
            $this->commit();
            $this->autoCommit = true;
        } finally {
            flock($this->dbLock, LOCK_UN);
        }
    }

    private function recover(): void
    {
        // Exclusive lock during recovery
        flock($this->dbLock, LOCK_EX);
        try {
            foreach ($this->wal->replay() as $operation) {
                if ($operation['operation'] === 'PUT') {
                    $this->storage->write($operation['id'], $operation['data']);
                } else if ($operation['operation'] === 'DELETE') {
                    $this->storage->delete($operation['id']);
                }
            }
        } catch (RuntimeException $e) {
            throw new RuntimeException('Failed to recover database state', 0, $e);
        } finally {
            flock($this->dbLock, LOCK_UN);
        }
    }
}
