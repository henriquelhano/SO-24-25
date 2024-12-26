#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "kvs.h"
#include "constants.h"

#include <unistd.h>
#include <fcntl.h>

static struct HashTable* kvs_table = NULL;


/// Calcula o timespec de um delay em milisegundos.
/// @param delay_ms Delay em millisegundos.
/// @return Timespec com o delay dado.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

int kvs_init() {
  if (kvs_table != NULL) {
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table();
  return kvs_table == NULL;
}

int kvs_terminate() {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  free_table(kvs_table);
  return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE], ThreadData *data) {
    if (kvs_table == NULL) {
        fprintf(stderr, "KVS state must be initialized\n");
        return 1;
    }
    int hashed[TABLE_SIZE] = {0};
    pthread_rwlock_rdlock(&data->rwlock);
    for (size_t i = 0; i < num_pairs; i++) {
        int posicao = hash(keys[i]);
        if (hashed[posicao] == 0) {
            pthread_rwlock_wrlock(&data->rwlock_array[posicao]);
            hashed[posicao] = 1;
        }
    }

    for (size_t i = 0; i < num_pairs; i++) {
        if (write_pair(kvs_table, keys[i], values[i]) != 0) {
            fprintf(stderr, "Fail to write keypair (%s,%s)\n", keys[i], values[i]);
        }
    }

    
    for (int i = 0; i < TABLE_SIZE; i++) {
        if (hashed[i] == 1) {
            pthread_rwlock_unlock(&data->rwlock_array[i]);
            hashed[i] = 0;
        }
    }
    pthread_rwlock_unlock(&data->rwlock);

    return 0;
}



int compare_keys(const void *a, const void *b) {
    return strcmp(*(const char **)a, *(const char **)b);  // Comparar duas chaves
}


int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int output_fd, ThreadData *data) {
    if (kvs_table == NULL) {
        fprintf(stderr, "KVS state must be initialized\n");
        return 1;
    }

    // Criar um array de ponteiros para as chaves
    char *keys_ptr[num_pairs];
    for (size_t i = 0; i < num_pairs; i++) {
        keys_ptr[i] = keys[i];  // Armazena os endereços das chaves
    }
    
    qsort(keys_ptr, num_pairs, sizeof(char *), compare_keys);

    int hashed[TABLE_SIZE] = {0};
    pthread_rwlock_rdlock(&data->rwlock);
    for (size_t i = 0; i < num_pairs; i++) {
        int position = hash(keys[i]);
        if (hashed[position] == 0) {
            pthread_rwlock_rdlock(&data->rwlock_array[position]);
            hashed[position] = 1;
        }
    }

    // Agora, as chaves estão ordenadas, então processa-as
    write(output_fd, "[", 1);
    for (size_t i = 0; i < num_pairs; i++) {
        // Usa keys_ptr[i] para aceder à chave ordenada
        char *result = read_pair(kvs_table, keys[i]);
        char buffer[MAX_STRING_SIZE];
        if (result == NULL) {
            sprintf(buffer, "(%s,KVSERROR)", keys_ptr[i]);
            write(output_fd, buffer, strlen(buffer));
        } else {
            sprintf(buffer, "(%s,%s)", keys_ptr[i], result);
            write(output_fd, buffer, strlen(buffer));
        }
        free(result);
    }
    write(output_fd, "]\n", 2);
        
    for (int i = 0; i < TABLE_SIZE; i++) {
        if (hashed[i] == 1) {
            pthread_rwlock_unlock(&data->rwlock_array[i]);
            hashed[i] = 0;
        }
    }
    pthread_rwlock_unlock(&data->rwlock);

    return 0;
}


int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int output_fd, ThreadData *data) {
    if (kvs_table == NULL) {
        fprintf(stderr, "KVS state must be initialized\n");
        return 1;
    }

    int hashed[TABLE_SIZE] = {0};
    pthread_rwlock_rdlock(&data->rwlock);
    for (size_t i = 0; i < num_pairs; i++) {
        int position = hash(keys[i]);
        if (hashed[position] == 0) {
            pthread_rwlock_wrlock(&data->rwlock_array[position]);
            hashed[position] = 1;
        }
    }

    int has_error = 0;
    for (size_t i = 0; i < num_pairs; i++) {
        if (delete_pair(kvs_table, keys[i]) != 0) {
            if (!has_error) {
                write(output_fd, "[", 1);  // Escreve o parentisis de abertura apenas uma vez
                has_error = 1;
            }
            char buffer[MAX_STRING_SIZE];
            sprintf(buffer, "(%s,KVSMISSING)", keys[i]);
            write(output_fd, buffer, strlen(buffer));
        }
    }

    if (has_error) {
        write(output_fd, "]\n", 2);  // Fecha os parentisis se houver erros
    }

    for (int i = 0; i < TABLE_SIZE; i++) {
        if (hashed[i] == 1) {
            pthread_rwlock_unlock(&data->rwlock_array[i]);
            hashed[i] = 0;
        }
    }
    pthread_rwlock_unlock(&data->rwlock);

    return 0;
}


void kvs_show(int output_fd, ThreadData *data) {
    pthread_rwlock_wrlock(&data->rwlock);
    for (int i = 0; i < TABLE_SIZE; i++) {
        KeyNode *keyNode = kvs_table->table[i];
        
        while (keyNode != NULL) {
            int len = snprintf(NULL, 0, "(%s, %s)\n", keyNode->key, keyNode->value);
            char buffer[len + 1];
            snprintf(buffer, sizeof(buffer), "(%s, %s)\n", keyNode->key, keyNode->value);
            write(output_fd, buffer, strlen(buffer));
            keyNode = keyNode->next;
        }
    }
    pthread_rwlock_unlock(&data->rwlock);
}

// kvs_show mas sem o rwlock porque o processo filho é de thread único
void kvs_show_backup(int output_fd) {
    for (int i = 0; i < TABLE_SIZE; i++) {
        KeyNode *keyNode = kvs_table->table[i];
        
        while (keyNode != NULL) {
            int len = snprintf(NULL, 0, "(%s, %s)\n", keyNode->key, keyNode->value);
            char buffer[len + 1];
            snprintf(buffer, sizeof(buffer), "(%s, %s)\n", keyNode->key, keyNode->value);
            write(output_fd, buffer, strlen(buffer));
            keyNode = keyNode->next;
        }
    }
}


int kvs_backup(int backup, const char *output_dir, const char *job_name) {
    char backup_output_path[MAX_JOB_FILE_NAME_SIZE];

    snprintf(backup_output_path, MAX_JOB_FILE_NAME_SIZE, "%s/%.*s-%d.bck", output_dir, (int)(strlen(job_name) - 4), job_name, backup);

    int backup_fd = open(backup_output_path, O_CREAT | O_WRONLY, 0644);

    kvs_show_backup(backup_fd);
    close(backup_fd);
    
    return 0;
}


void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}