<?php

require_once __DIR__ . '/../vendor/autoload.php';

// Initialize the database
$db = new PHPNoSQL\Database(dirname(__DIR__) . '/database');

// Basic CRUD operations
$id = $db->users->save(['name' => 'John', 'age' => 30]);
$user = $db->users->find($id);
$db->users->delete($id);

// Get all documents in a collection
$allUsers = $db->users->all();

// Create an index
$db->users->createIndex('age_index', ['age']);

// Query using an index
$youngUsers = $db->users->query('age_index', ['age' => [18, 25]]);

// Magic findBy methods
$johnsUsers = $db->users->findByName('John');

// Batch operations
$db->users->beginBatch();
$db->users->save(['name' => 'User 1']);
$db->users->save(['name' => 'User 2']);
$db->users->endBatch();