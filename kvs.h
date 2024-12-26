#ifndef KEY_VALUE_STORE_H
#define KEY_VALUE_STORE_H

#define TABLE_SIZE 26

#include "constants.h"

#include <stddef.h>
#include <pthread.h>


typedef struct {
    char files[MAX_WRITE_SIZE][MAX_JOB_FILE_NAME_SIZE];// Lista de arquivos .job
    int count;                                         // Número total de arquivos
    int current_index;                                 // Índice do próximo arquivo a processar
    pthread_mutex_t file_mutex;                        // Mutex para proteger o acesso ao índice
    const char *output_dir;                            // Diretório de saída
    int max_backups;                                   // Número máximo de backups
    pthread_rwlock_t rwlock;                           // Rwlock global para o proteger o comando show
    pthread_rwlock_t rwlock_array[26];                 // Rwlocks para cada letra do alfabeto
    int active_backups;                                // Backups em atividade
    pthread_mutex_t backup_mutex;                      // Protege o acesso ao número de backups ativos
} ThreadData;

typedef struct KeyNode {
    char *key;
    char *value;
    struct KeyNode *next;
} KeyNode;


typedef struct HashTable {
    KeyNode *table[TABLE_SIZE];
} HashTable;

/// Função hash baseada na chave inicial.
/// @param key Letra minuscula.
/// @return hash.
/// NOTE: Não é uma função ideal, mas cumpre os propósitos
int hash(const char *key);

/// Cria uma nova hash-table
/// @return da hash table criada, NULL caso haja problemas na criação
struct HashTable *create_hash_table();

/// Adiciona um novo par chave valor à hash-table.
/// @param ht Hash table a ser alterada.
/// @param key Chave a ser escritra.
/// @param value Valor a ser escrito.
/// @return 0 se foi adicionado com sucesso, 1 caso contrário.
int write_pair(HashTable *ht, const char *key, const char *value);

/// Lê o valor da chave dada.
/// @param ht Hash table a ver a chave.
/// @param key Chave do par a ser lida.
/// @return 0 se foi lido com sucesso, 1 caso contrário.
char* read_pair(HashTable *ht, const char *key);

/// Apaga o nó da hash table.
/// @param list Hash Table a ser apagado o valor.
/// @param key Chave do par a apagar.
/// @return 0 se foi retirado com sucesso, 1 caso contrário.
int delete_pair(HashTable *ht, const char *key);


/// Liberta a memória da hashtable.
/// @param ht Hash table a ser apagada.
void free_table(HashTable *ht);


#endif  // KVS_H
