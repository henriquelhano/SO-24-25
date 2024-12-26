#ifndef KVS_PARSER_H
#define KVS_PARSER_H

#include <stddef.h>
#include "constants.h"

enum Command {
  CMD_WRITE,
  CMD_READ,
  CMD_DELETE,
  CMD_SHOW,
  CMD_WAIT,
  CMD_BACKUP,
  CMD_HELP,
  CMD_EMPTY,
  CMD_INVALID,
  EOC  // Fim dos comandos
};

/// Lê uma linha e retorna o comando correspondente.
/// @param fd Ficheiro ao qual se lê o comando.
/// @return O comando lido.
enum Command get_next(int fd);

/// Lê um comando WRITE
/// @param fd Ficheiro ao qual se lê o comando.
/// @param keys Array de chaves.
/// @param values Array dos valores.
/// @param max_pairs Numero de pares a ser escrito.
/// @param max_string_size Tamanho maximo das chaves e valores.
/// @return 0 se o comando foi lido com sucesso, 1 caso contrário.
size_t parse_write(int fd, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE], size_t max_pairs, size_t max_string_size);

/// Lê um comando READ ou DELETE
/// @param fd Ficheiro ao qual se lê o comando.
/// @param keys Array de chaves.
/// @param max_keys Numero de chaves a ser lidos ou apagados.
/// @param max_string_size Tamanho maximo das chaves e valores.
/// @return Número de chaves lidas ou apagadas. 0 em caso de falha.
size_t parse_read_delete(int fd, char keys[][MAX_STRING_SIZE], size_t max_keys, size_t max_string_size);

/// Lê um comando WAIT
/// @param fd Ficheiro ao qual se lê o comando.
/// @param delay Ponteiro para a variável na qual armazena o delay.
/// @param thread_id Ponteiro para a variável na qual armazena o ID do thread. Pode não ser definido.
/// @return 0 se nenhuma thread foi especificada, 1 se uma thread foi especificada, -1 em caso de erro.
int parse_wait(int fd, unsigned int *delay, unsigned int *thread_id);

#endif  // KVS_PARSER_H
