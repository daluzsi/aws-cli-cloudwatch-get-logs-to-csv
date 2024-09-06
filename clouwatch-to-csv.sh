#!/bin/bash

# Parâmetros da consulta
LOG_GROUP="log-group-name"
START_TIME=1700756909519
END_TIME=1700769015
QUERY_STRING="fields @timestamp, @message | limit 10000"
AWS_PROFILE=profile-name
AWS_REGION="us-east-1"  # Altere conforme necessário

# Nome do arquivo CSV de saída e arquivo de checkpoint para retomar a execução
OUTPUT_FILE="log_results.csv"
CHECKPOINT_FILE="checkpoint.txt"
LOG_FILE="error_log.txt"

# Função para registrar erros e sair
log_error() {
  echo "Erro: $1" | tee -a "$LOG_FILE"
  exit 1
}

# Escrever cabeçalho do CSV, apenas se o arquivo não existir
if [[ ! -f "$OUTPUT_FILE" ]]; then
  echo "timestamp,message" > "$OUTPUT_FILE"
fi

# Função para carregar checkpoint, se existir
load_checkpoint() {
  if [[ -f "$CHECKPOINT_FILE" ]]; then
    source "$CHECKPOINT_FILE"
    echo "Retomando do checkpoint: QUERY_ID=$QUERY_ID, NEXT_TOKEN=$NEXT_TOKEN"
  else
    # Se não houver checkpoint, iniciar nova query
    QUERY_ID=$(aws logs start-query \
      --log-group-name "$LOG_GROUP" \
      --start-time "$START_TIME" \
      --end-time "$END_TIME" \
      --query-string "$QUERY_STRING" \
      --limit 10000 \
      --region "$AWS_REGION" \
      --profile "$AWS_PROFILE" \
      --query "queryId" \
      --output text)

    if [[ $? -ne 0 || -z "$QUERY_ID" ]]; then
      log_error "Falha ao iniciar a consulta"
    fi

    # Salvar checkpoint inicial
    echo "QUERY_ID=$QUERY_ID" > "$CHECKPOINT_FILE"
    NEXT_TOKEN=""
  fi
}

# Carregar checkpoint ou iniciar nova consulta
load_checkpoint

# Loop para buscar todos os resultados paginados
while true; do
  # Buscar resultados da query
  if [[ -z "$NEXT_TOKEN" ]]; then
    RESPONSE=$(aws logs get-query-results \
      --query-id "$QUERY_ID" \
      --region "$AWS_REGION" \
      --profile "$AWS_PROFILE")
  else
    RESPONSE=$(aws logs get-query-results \
      --query-id "$QUERY_ID" \
      --next-token "$NEXT_TOKEN" \
      --region "$AWS_REGION" \
      --profile "$AWS_PROFILE")
  fi

  # Verificar se houve erro ao buscar resultados
  if [[ $? -ne 0 ]]; then
    log_error "Erro ao buscar resultados para QUERY_ID=$QUERY_ID, NEXT_TOKEN=$NEXT_TOKEN"
  fi

  # Verificar se a consulta ainda está em andamento
  STATUS=$(echo "$RESPONSE" | jq -r '.status')
  if [[ "$STATUS" == "Running" ]]; then
    echo "Aguardando finalização da query..."
    sleep 5
    continue
  fi

  # Extrair resultados e adicionar ao CSV
  echo "$RESPONSE" | jq -r '.results[] | [.[] | select(.field == "@timestamp" or .field == "@message") | .value] | @csv' >> "$OUTPUT_FILE"

  # Verificar se há um nextToken para continuar buscando mais registros
  NEXT_TOKEN=$(echo "$RESPONSE" | jq -r '.nextToken')

  # Atualizar checkpoint com o próximo token
  echo "QUERY_ID=$QUERY_ID" > "$CHECKPOINT_FILE"
  if [[ "$NEXT_TOKEN" != "null" ]]; then
    echo "NEXT_TOKEN=$NEXT_TOKEN" >> "$CHECKPOINT_FILE"
  fi

  # Se não houver nextToken, parar o loop
  if [[ "$NEXT_TOKEN" == "null" ]]; then
    echo "Todos os registros foram recuperados com sucesso."
    # Remover arquivo de checkpoint após sucesso
    rm -f "$CHECKPOINT_FILE"
    break
  fi
done

echo "Registros salvos no arquivo $OUTPUT_FILE"
