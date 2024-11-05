<?php

declare(strict_types=1);

namespace PHPNoSQL;

/**
 * A Wrapper for easy usage of the database.
 */
class Database
{
    private PHPNoSQL $db;
    private QueryEngine $queryEngine;
    private string $currentCollection = 'default';

    public function __construct(string $dbPath = 'db/nosql')
    {
        $this->db = new PHPNoSQL($dbPath);
        $this->queryEngine = new QueryEngine($dbPath . '/index');
    }

    /**
     * Set the current collection context
     */
    public function collection(string $name): self
    {
        $this->currentCollection = $name;
        return $this;
    }

    /**
     * Save a document with automatic ID generation
     */
    public function save(array $document): string
    {
        $id = $this->generateId();
        $this->db->put($this->getFullId($id), $document);
        return $id;
    }

    /**
     * Find a document by ID
     */
    public function find(string $id): ?array
    {
        return $this->db->get($this->getFullId($id));
    }

    /**
     * Delete a document by ID
     */
    public function delete(string $id): void
    {
        $this->db->delete($this->getFullId($id));
    }

    /**
     * Get all documents in the current collection
     */
    public function all(): array
    {
        $prefix = $this->currentCollection . ':';
        $documents = [];
        
        foreach ($this->db->getAllDocuments() as $id => $doc) {
            if (str_starts_with($id, $prefix)) {
                $documents[substr($id, strlen($prefix))] = $doc;
            }
        }
        
        return $documents;
    }

    /**
     * Create an index for faster querying
     */
    public function createIndex(string $name, array $fields): void
    {
        $this->queryEngine->createIndex($this->currentCollection . '_' . $name, $fields);
    }

    /**
     * Query documents using an index
     */
    public function query(string $indexName, array $conditions): array
    {
        return iterator_to_array(
            $this->queryEngine->query($this->currentCollection . '_' . $indexName, $conditions)
        );
    }

    /**
     * Start a batch operation
     */
    public function beginBatch(): void
    {
        $this->db->beginBatch();
    }

    /**
     * End and commit a batch operation
     */
    public function endBatch(): void
    {
        $this->db->endBatch();
    }

    /**
     * Magic method to handle dynamic property access for collections
     */
    public function __get(string $name): self
    {
        return $this->collection($name);
    }

    /**
     * Magic method to handle dynamic method calls
     */
    public function __call(string $name, array $arguments): mixed
    {
        // Handle findBy{Field} methods
        if (str_starts_with($name, 'findBy')) {
            $field = lcfirst(substr($name, 6));
            if (count($arguments) !== 1) {
                throw new \RuntimeException("findBy{$field} requires exactly one argument");
            }
            
            // Create a temporary index if it doesn't exist
            $indexName = "temp_{$field}_index";
            try {
                $this->createIndex($indexName, [$field]);
            } catch (\RuntimeException $e) {
                // Index might already exist
            }
            
            return $this->query($indexName, [$field => $arguments[0]]);
        }

        throw new \RuntimeException("Unknown method: $name");
    }

    /**
     * Generate a unique ID for a document
     */
    private function generateId(): string
    {
        return uniqid(more_entropy: true);
    }

    /**
     * Get the full ID including collection prefix
     */
    private function getFullId(string $id): string
    {
        return $this->currentCollection . ':' . $id;
    }
}
