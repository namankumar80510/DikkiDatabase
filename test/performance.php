#!/usr/bin/env php
<?php

declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

use PHPNoSQL\PHPNoSQL;
use PHPNoSQL\QueryEngine;

class PerformanceTestConfig
{
    public string $dbPath;
    public function __construct(
        public readonly string $nosqlPath = __DIR__ . '/../perftest_db/nosql',
        public readonly int $numDocs = 10000,
        public readonly int $numReads = 10000,
        public readonly int $queryCount = 1000,
        public readonly int $batchSize = 1000,
        public readonly int $progressInterval = 1000
    ) {
        $this->dbPath = $this->nosqlPath;
    }
}

class PerformanceMetrics
{
    private float $startTime;
    private array $metrics = [];

    public function startTimer(): void
    {
        $this->startTime = microtime(true);
    }

    public function recordMetric(string $name, int $operations): void
    {
        $duration = microtime(true) - $this->startTime;
        $this->metrics[$name] = [
            'duration' => $duration,
            'operations' => $operations,
            'avg_time_ms' => ($duration / $operations) * 1000
        ];
    }

    public function getMetric(string $name): array
    {
        return $this->metrics[$name] ?? [];
    }

    public function getAllMetrics(): array
    {
        return $this->metrics;
    }
}

class TestDataGenerator
{
    public function generateDocument(int $i): array
    {
        return [
            'name' => "Test Document $i",
            'value' => strval(rand(1, 10000000)),  // Increased range
            'tags' => ['test', 'performance', strval(rand(1, 1000))],  // Increased range
            'timestamp' => time()
        ];
    }
}

class PerformanceTest
{
    private PerformanceMetrics $nosqlMetrics;
    private PHPNoSQL $db;
    private QueryEngine $queryEngine;
    private TestDataGenerator $dataGenerator;

    public function __construct(
        private readonly PerformanceTestConfig $config
    ) {
        $this->nosqlMetrics = new PerformanceMetrics();
        $this->db = new PHPNoSQL($config->dbPath);
        $this->queryEngine = new QueryEngine($config->dbPath . '/index');
        $this->dataGenerator = new TestDataGenerator();
    }

    public function run(): void
    {
        $this->setupDatabase();
        $this->runInsertTest();
        $this->runReadTest();
        $this->runQueryTest();
        $this->reportMemoryUsage();
        $this->comparePerformance();
    }

    private function setupDatabase(): void
    {
        $this->queryEngine->createIndex('value_index', ['value']);
        echo "Performance test initialized with {$this->config->numDocs} documents\n";
    }

    private function runInsertTest(): void
    {
        // NoSQL Insert Test
        echo "\nStarting NoSQL insert test...\n";
        $this->nosqlMetrics->startTimer();
        $nosqlErrors = 0;

        for ($i = 0; $i < $this->config->numDocs; $i += $this->config->batchSize) {
            $this->db->beginBatch();
            for ($j = 0; $j < $this->config->batchSize && ($i + $j) < $this->config->numDocs; $j++) {
                $docId = "doc_" . ($i + $j);
                $data = $this->dataGenerator->generateDocument($i + $j);
                try {
                    $this->db->put($docId, $data);
                    $this->queryEngine->updateIndex('value_index', $docId, $data);
                } catch (Exception $e) {
                    $nosqlErrors++;
                }
            }
            $this->db->endBatch();
        }
        $this->nosqlMetrics->recordMetric('insert', $this->config->numDocs);
    }

    private function runReadTest(): void
    {
        // NoSQL Read Test
        echo "\nStarting NoSQL read test...\n";
        $this->nosqlMetrics->startTimer();
        $nosqlHits = 0;

        for ($i = 0; $i < $this->config->numReads; $i++) {
            $randomId = "doc_" . rand(0, $this->config->numDocs - 1);
            try {
                $doc = $this->db->get($randomId);
                if ($doc !== null) $nosqlHits++;
            } catch (Exception $e) {
                // Skip error handling for brevity
            }
        }
        $this->nosqlMetrics->recordMetric('read', $this->config->numReads);
    }

    private function runQueryTest(): void
    {
        // NoSQL Query Test
        echo "\nStarting NoSQL query test...\n";
        $this->nosqlMetrics->startTimer();

        for ($i = 0; $i < $this->config->queryCount; $i++) {
            $minValue = strval(rand(1, 9000000));
            $maxValue = strval($minValue + 1000000);
            try {
                iterator_to_array($this->queryEngine->query('value_index', [
                    'value' => [$minValue, $maxValue]
                ]));
            } catch (Exception $e) {
                // Skip error handling for brevity
            }
        }
        $this->nosqlMetrics->recordMetric('query', $this->config->queryCount);
    }

    private function reportMemoryUsage(): void
    {
        echo "\nMemory Usage:\n";
        echo "Peak: " . number_format(memory_get_peak_usage() / 1024 / 1024, 2) . " MB\n";
        echo "Current: " . number_format(memory_get_usage() / 1024 / 1024, 2) . " MB\n";
    }

    private function comparePerformance(): void
    {
        echo "\nPerformance Results (NoSQL):\n";
        echo str_repeat('-', 50) . "\n";
        printf("| %-15s | %-30s |\n", 'Operation', 'Performance');
        echo str_repeat('-', 50) . "\n";

        $operations = ['insert', 'read', 'query'];
        foreach ($operations as $op) {
            $nosql = $this->nosqlMetrics->getMetric($op);
            printf(
                "| %-15s | %6.2f ms/op (%5.2fs) |\n",
                ucfirst($op),
                $nosql['avg_time_ms'],
                $nosql['duration']
            );
        }
        echo str_repeat('-', 50) . "\n";
    }
}

// Run the performance test
try {
    $config = new PerformanceTestConfig();
    $test = new PerformanceTest($config);
    $test->run();
} catch (Exception $e) {
    echo "Fatal error: " . $e->getMessage() . "\n";
    exit(1);
}
