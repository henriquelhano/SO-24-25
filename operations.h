#ifndef KVS_OPERATIONS_H
#define KVS_OPERATIONS_H

#include <stddef.h>
#include "kvs.h"

/// Inicializa o KVS.
/// @return 0 se o KVS foi inicializado com sucesso, 1 caso contrário.
int kvs_init();

/// Destroys the KVS state.
/// @return 0 se o KVS foi terminado com sucesso, 1 caso contrário.
int kvs_terminate();

/// Escreve um par chave valor no KVS. Se a chave já existe o valor é atualizado.
/// @param num_pairs Número de pares a ser escrito.
/// @param keys Array das chaves.
/// @param values Array dos valores.
/// @param data Estrutura thread que faz a operação
/// @return 0 se o KVS escreveu com sucesso, 1 caso contrário.
int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE], ThreadData *data);

/// Lê valores do KVS.
/// @param num_pairs Número de pares a ler.
/// @param keys Array de chaves.
/// @param output_fd Ficheiro ao qual escrever o par chave-valor que se leu.
/// @param data Estrutura thread que faz a operação
/// @return 0 se o KVS leu com sucesso, 1 caso contrário.
int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int output_fd, ThreadData *data);

/// Apagar valores do KVS.
/// @param num_pairs Número de pares a apagar.
/// @param keys Array de chaves.
/// @param data Estrutura thread que faz a operação
/// @return 0 se o KVS apagou com sucesso, 1 caso contrário.
int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int output_fd, ThreadData *data);

/// Escreve o estado do KVS.
/// @param output_fd Ficheiro ao qual escrever o estado do KVS.
void kvs_show(int output_fd, ThreadData *data);

/// Cria um backup do KVS e armazena no ficheiro backup correspondente
/// @return 0 se o backup teve sucesso, 1 caso contrário.
int kvs_backup(int backup, const char *output_dir, const char *job_name);

/// Espera até ao último backup ser chamado.
void kvs_wait_backup();

/// Espera um determinado tempo.
/// @param delay_us Delay em millisegundos.
void kvs_wait(unsigned int delay_ms);

#endif  // KVS_OPERATIONS_H
