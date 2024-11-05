<?php

declare(strict_types=1);

namespace PHPNoSQL;

class BloomFilter
{
    private $bitArray;
    private $numHashes;
    private $size;

    public function __construct(int $expectedItems, float $falsePositiveRate)
    {
        $this->size = $this->optimalSize($expectedItems, $falsePositiveRate);
        $this->numHashes = $this->optimalHashes($expectedItems, $this->size);
        $this->bitArray = array_fill(0, ($this->size + 63) >> 6, 0);
    }

    public function add(string $item): void
    {
        $hashes = $this->getHashes($item);
        foreach ($hashes as $hash) {
            $pos = $hash % $this->size;
            $this->bitArray[$pos >> 6] |= 1 << ($pos & 63);
        }
    }

    public function mightContain(string $item): bool
    {
        $hashes = $this->getHashes($item);
        foreach ($hashes as $hash) {
            $pos = $hash % $this->size;
            if (!($this->bitArray[$pos >> 6] & (1 << ($pos & 63)))) {
                return false;
            }
        }
        return true;
    }

    private function getHashes(string $item): array
    {
        $hashes = [];
        $hash1 = crc32($item);
        $hash2 = crc32(strrev($item));

        for ($i = 0; $i < $this->numHashes; $i++) {
            $hashes[] = abs($hash1 + $i * $hash2);
        }
        return $hashes;
    }

    private function optimalSize(int $n, float $p): int
    {
        return (int)ceil(- ($n * log($p)) / (log(2) * log(2)));
    }

    private function optimalHashes(int $n, int $m): int
    {
        return (int)round(($m / $n) * log(2));
    }
}
