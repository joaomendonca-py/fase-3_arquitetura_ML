# CONFIGURAR GITHUB SECRETS PARA ACTIONS

## PASSO A PASSO:

### 1. ACESSAR CONFIGURACOES DO REPOSITORIO
- Va para: https://github.com/joaomendonca-py/fase-3_arquitetura_ML
- Clique em "Settings" (no menu superior)
- No menu lateral, clique em "Secrets and variables" 
- Clique em "Actions"

### 2. ADICIONAR SECRETS AWS
Clique em "New repository secret" e adicione cada um:

**Nome**: `AWS_ACCESS_KEY_ID`
**Valor**: Sua AWS Access Key ID (do ~/.aws/credentials)

**Nome**: `AWS_SECRET_ACCESS_KEY` 
**Valor**: Sua AWS Secret Access Key (do ~/.aws/credentials)

### 3. ADICIONAR SECRETS S3 BUCKETS
**Nome**: `TC_S3_RAW`
**Valor**: `imdb-raw-data-718942601863`

**Nome**: `TC_S3_TRUSTED`
**Valor**: `imdb-trusted-data-718942601863`

**Nome**: `TC_S3_REFINED` 
**Valor**: `imdb-refined-data-718942601863`

**Nome**: `TC_S3_MODELS`
**Valor**: `imdb-ml-models-718942601863`

### 4. VERIFICAR AWS CREDENTIALS
Para pegar suas credenciais AWS, execute:
```bash
cat ~/.aws/credentials
```

### 5. TESTAR GITHUB ACTIONS
Apos configurar todos os secrets:
1. Faca um pequeno commit
2. Faca push para main
3. Va para a aba "Actions" no GitHub
4. Verifique se o workflow executou

### 6. ENVIRONMENTS (OPCIONAL)
Para deploy em producao, configure o environment "production":
- Settings > Environments
- Clique em "New environment"
- Nome: "production"
- Configure protection rules se necessario

## VERIFICAR SE FUNCIONA:
Apos configurar tudo, o workflow deveria executar automaticamente quando voce fizer push para main.
