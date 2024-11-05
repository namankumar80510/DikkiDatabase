<?php

declare(strict_types=1);

namespace PHPNoSQL;

use RuntimeException;
use Generator;
use SplFileObject;

/**
 * Query engine with indexing support and concurrency
 */
final class QueryEngine
{
    private array $indexes = [];
    private const MAX_INDEX_CACHE = 10000;
    private $indexLock;

    public function __construct(private readonly string $indexPath)
    {
        if (!is_dir($indexPath)) {
            if (!mkdir($indexPath, 0777, true)) {
                throw new RuntimeException("Failed to create index directory: $indexPath");
            }
        }

        $this->indexLock = fopen($indexPath . '/index.lock', 'c+');
        if ($this->indexLock === false) {
            throw new RuntimeException("Failed to create index lock file");
        }
    }

    public function createIndex(string $name, array $fields): void
    {
        if (empty($name) || empty($fields)) {
            throw new RuntimeException('Index name and fields cannot be empty');
        }

        // Exclusive lock for creating index
        flock($this->indexLock, LOCK_EX);
        try {
            $this->indexes[$name] = [
                'fields' => $fields,
                'data' => []
            ];
        } finally {
            flock($this->indexLock, LOCK_UN);
        }
    }

    public function updateIndex(string $name, string $id, array $document): void
    {
        // Exclusive lock for updating index
        flock($this->indexLock, LOCK_EX);
        try {
            if (!isset($this->indexes[$name])) {
                return;
            }

            $indexData = [];
            foreach ($this->indexes[$name]['fields'] as $field) {
                $indexData[$field] = $document[$field] ?? null;
            }

            $this->indexes[$name]['data'][$id] = $indexData;

            // Maintain index size
            if (count($this->indexes[$name]['data']) > self::MAX_INDEX_CACHE) {
                array_shift($this->indexes[$name]['data']);
            }
        } finally {
            flock($this->indexLock, LOCK_UN);
        }
    }

    public function removeFromIndex(string $name, string $id): void
    {
        // Exclusive lock for removing from index
        flock($this->indexLock, LOCK_EX);
        try {
            if (isset($this->indexes[$name])) {
                unset($this->indexes[$name]['data'][$id]);
            }
        } finally {
            flock($this->indexLock, LOCK_UN);
        }
    }

    public function query(string $indexName, array $conditions): Generator
    {
        if (!isset($this->indexes[$indexName])) {
            throw new RuntimeException("Index not found: $indexName");
        }

        // Shared lock for querying
        flock($this->indexLock, LOCK_SH);
        try {
            foreach ($this->indexes[$indexName]['data'] as $id => $indexData) {
                if (empty($conditions)) {
                    yield $id;
                    continue;
                }

                $matches = true;
                foreach ($conditions as $field => $value) {
                    if (!isset($indexData[$field])) {
                        $matches = false;
                        break;
                    }

                    // Handle range queries
                    if (is_array($value)) {
                        if (count($value) !== 2) {
                            throw new RuntimeException("Range query must have exactly 2 values");
                        }
                        [$min, $max] = $value;
                        if ($indexData[$field] < $min || $indexData[$field] > $max) {
                            $matches = false;
                            break;
                        }
                    }
                    // Handle exact match or substring search
                    else {
                        if (!is_string($value) || !is_string($indexData[$field])) {
                            throw new RuntimeException("Query value and indexed value must be strings for text search");
                        }
                        if (stripos($indexData[$field], $value) === false) {
                            $matches = false;
                            break;
                        }
                    }
                }
                if ($matches) {
                    yield $id;
                }
            }
        } finally {
            flock($this->indexLock, LOCK_UN);
        }
    }
}
