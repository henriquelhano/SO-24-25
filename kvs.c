#include "kvs.h"
#include "string.h"

#include <stdlib.h>
#include <ctype.h>

/// Função hash baseada na chave inicial.
/// @param key Letra minuscula.
/// @return hash.
/// NOTE: Não é uma função ideal, mas cumpre os propósitos
int hash(const char *key) {
    int firstLetter = tolower(key[0]);
    if (firstLetter >= 'a' && firstLetter <= 'z') {
        return firstLetter - 'a';
    } else if (firstLetter >= '0' && firstLetter <= '9') {
        return firstLetter - '0';
    }
    return -1; // Índice invalido para strings que não são letras
}

    
struct HashTable* create_hash_table() {
  HashTable *ht = malloc(sizeof(HashTable));
  if (!ht) return NULL;
  for (int i = 0; i < TABLE_SIZE; i++) {
      ht->table[i] = NULL;
  }
  return ht;
}


int write_pair(HashTable *ht, const char *key, const char *value) {
    int index = hash(key);
    KeyNode *keyNode = ht->table[index];

    // Search for the key node
    while (keyNode != NULL) {
        if (strcmp(keyNode->key, key) == 0) {
            free(keyNode->value);
            keyNode->value = strdup(value);
            return 0;
        }
        keyNode = keyNode->next; // Move to the next node
    }

    // Key not found, create a new key node
    keyNode = malloc(sizeof(KeyNode));
    keyNode->key = strdup(key); // Allocate memory for the key
    keyNode->value = strdup(value); // Allocate memory for the value
    keyNode->next = ht->table[index]; // Link to existing nodes
    ht->table[index] = keyNode; // Place new key node at the start of the list
    return 0;
}



char* read_pair(HashTable *ht, const char *key) {
    int index = hash(key);
    KeyNode *keyNode = ht->table[index];
    char* value;

    while (keyNode != NULL) {
        if (strcmp(keyNode->key, key) == 0) {
            value = strdup(keyNode->value);
            return value; // Devolve cópia do valor caso exista
        }
        keyNode = keyNode->next; // Move para o seguinte
    }
    return NULL; // Chave não encontrada
}


int delete_pair(HashTable *ht, const char *key) {
    int indice = hash(key);
    KeyNode *keyNode = ht->table[indice];
    KeyNode *prevNode = NULL;

    // Procurar o determinado nó
    while (keyNode != NULL) {
        if (strcmp(keyNode->key, key) == 0) {
            // Existe, logo apaga este nó (se for o primeiro par)
            if (prevNode == NULL) {
                ht->table[indice] = keyNode->next; 
            } else {
                // Nó a apagar caso nao seja o primeiro
                prevNode->next = keyNode->next; 
            }
            // Liberta a memória do nó, do valor e da chave
            free(keyNode->key);
            free(keyNode->value);
            free(keyNode); 
            return 0; 
        }
        prevNode = keyNode; // Move prevNode para o atual
        keyNode = keyNode->next; // Move para o seguinte
    }
    return 1;
}


void free_table(HashTable *ht) {
    for (int i = 0; i < TABLE_SIZE; i++) {
        KeyNode *keyNode = ht->table[i];
        while (keyNode != NULL) {
            KeyNode *temp = keyNode;
            keyNode = keyNode->next;
            free(temp->key);
            free(temp->value);
            free(temp);
        }
    }
    free(ht);
}