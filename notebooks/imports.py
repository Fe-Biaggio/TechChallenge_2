import basedosdados as bd

billing_id = <seu_billing_id>

query = """
  SELECT
    dados.ano as ano,
    dados.rede as rede,
    dados.taxa_alfabetizacao as taxa_alfabetizacao,
    dados.meta_alfabetizacao_2024 as meta_alfabetizacao_2024,
    dados.meta_alfabetizacao_2025 as meta_alfabetizacao_2025,
    dados.meta_alfabetizacao_2026 as meta_alfabetizacao_2026,
    dados.meta_alfabetizacao_2027 as meta_alfabetizacao_2027,
    dados.meta_alfabetizacao_2028 as meta_alfabetizacao_2028,
    dados.meta_alfabetizacao_2029 as meta_alfabetizacao_2029,
    dados.meta_alfabetizacao_2030 as meta_alfabetizacao_2030,
    dados.percentual_participacao as percentual_participacao
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_brasil` AS dados
"""

bd.read_sql(query = query, billing_project_id = billing_id)

import basedosdados as bd

billing_id = <seu_billing_id>

query = """
  WITH 
dicionario_serie AS (
    SELECT
        chave AS chave_serie,
        valor AS descricao_serie
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'serie'
        AND id_tabela = 'uf'
),
dicionario_rede AS (
    SELECT
        chave AS chave_rede,
        valor AS descricao_rede
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'rede'
        AND id_tabela = 'uf'
)
SELECT
    dados.ano as ano,
    dados.sigla_uf AS sigla_uf,
    diretorio_sigla_uf.nome AS sigla_uf_nome,
    descricao_serie AS serie,
    descricao_rede AS rede,
    dados.taxa_alfabetizacao as taxa_alfabetizacao,
    dados.media_portugues as media_portugues,
    dados.proporcao_aluno_nivel_0 as proporcao_aluno_nivel_0,
    dados.proporcao_aluno_nivel_1 as proporcao_aluno_nivel_1,
    dados.proporcao_aluno_nivel_2 as proporcao_aluno_nivel_2,
    dados.proporcao_aluno_nivel_3 as proporcao_aluno_nivel_3,
    dados.proporcao_aluno_nivel_4 as proporcao_aluno_nivel_4,
    dados.proporcao_aluno_nivel_5 as proporcao_aluno_nivel_5,
    dados.proporcao_aluno_nivel_6 as proporcao_aluno_nivel_6,
    dados.proporcao_aluno_nivel_7 as proporcao_aluno_nivel_7,
    dados.proporcao_aluno_nivel_8 as proporcao_aluno_nivel_8
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.uf` AS dados
LEFT JOIN (SELECT DISTINCT sigla,nome  FROM `basedosdados.br_bd_diretorios_brasil.uf`) AS diretorio_sigla_uf
    ON dados.sigla_uf = diretorio_sigla_uf.sigla
LEFT JOIN `dicionario_serie`
    ON dados.serie = chave_serie
LEFT JOIN `dicionario_rede`
    ON dados.rede = chave_rede
"""

bd.read_sql(query = query, billing_project_id = billing_id)

import basedosdados as bd

billing_id = <seu_billing_id>

query = """
  SELECT
    dados.ano as ano,
    dados.sigla_uf AS sigla_uf,
    diretorio_sigla_uf.nome AS sigla_uf_nome,
    dados.rede as rede,
    dados.taxa_alfabetizacao as taxa_alfabetizacao,
    dados.meta_alfabetizacao_2024 as meta_alfabetizacao_2024,
    dados.meta_alfabetizacao_2025 as meta_alfabetizacao_2025,
    dados.meta_alfabetizacao_2026 as meta_alfabetizacao_2026,
    dados.meta_alfabetizacao_2027 as meta_alfabetizacao_2027,
    dados.meta_alfabetizacao_2028 as meta_alfabetizacao_2028,
    dados.meta_alfabetizacao_2029 as meta_alfabetizacao_2029,
    dados.meta_alfabetizacao_2030 as meta_alfabetizacao_2030,
    dados.percentual_participacao as percentual_participacao
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_uf` AS dados
LEFT JOIN (SELECT DISTINCT sigla,nome  FROM `basedosdados.br_bd_diretorios_brasil.uf`) AS diretorio_sigla_uf
    ON dados.sigla_uf = diretorio_sigla_uf.sigla
"""

bd.read_sql(query = query, billing_project_id = billing_id)

import basedosdados as bd

billing_id = <seu_billing_id>

query = """
  SELECT
    dados.ano as ano,
    dados.id_municipio AS id_municipio,
    diretorio_id_municipio.nome AS id_municipio_nome,
    dados.rede as rede,
    dados.taxa_alfabetizacao as taxa_alfabetizacao,
    dados.meta_alfabetizacao_2024 as meta_alfabetizacao_2024,
    dados.meta_alfabetizacao_2025 as meta_alfabetizacao_2025,
    dados.meta_alfabetizacao_2026 as meta_alfabetizacao_2026,
    dados.meta_alfabetizacao_2027 as meta_alfabetizacao_2027,
    dados.meta_alfabetizacao_2028 as meta_alfabetizacao_2028,
    dados.meta_alfabetizacao_2029 as meta_alfabetizacao_2029,
    dados.meta_alfabetizacao_2030 as meta_alfabetizacao_2030,
    dados.nivel_alfabetizacao as nivel_alfabetizacao,
    dados.percentual_participacao as percentual_participacao
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_municipio` AS dados
LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
    ON dados.id_municipio = diretorio_id_municipio.id_municipio
"""

bd.read_sql(query = query, billing_project_id = billing_id)

import basedosdados as bd

billing_id = <seu_billing_id>

query = """
  WITH 
dicionario_serie AS (
    SELECT
        chave AS chave_serie,
        valor AS descricao_serie
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'serie'
        AND id_tabela = 'municipio'
),
dicionario_rede AS (
    SELECT
        chave AS chave_rede,
        valor AS descricao_rede
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'rede'
        AND id_tabela = 'municipio'
)
SELECT
    dados.ano as ano,
    dados.id_municipio AS id_municipio,
    diretorio_id_municipio.nome AS id_municipio_nome,
    descricao_serie AS serie,
    descricao_rede AS rede,
    dados.taxa_alfabetizacao as taxa_alfabetizacao,
    dados.media_portugues as media_portugues,
    dados.proporcao_aluno_nivel_0 as proporcao_aluno_nivel_0,
    dados.proporcao_aluno_nivel_1 as proporcao_aluno_nivel_1,
    dados.proporcao_aluno_nivel_2 as proporcao_aluno_nivel_2,
    dados.proporcao_aluno_nivel_3 as proporcao_aluno_nivel_3,
    dados.proporcao_aluno_nivel_4 as proporcao_aluno_nivel_4,
    dados.proporcao_aluno_nivel_5 as proporcao_aluno_nivel_5,
    dados.proporcao_aluno_nivel_6 as proporcao_aluno_nivel_6,
    dados.proporcao_aluno_nivel_7 as proporcao_aluno_nivel_7,
    dados.proporcao_aluno_nivel_8 as proporcao_aluno_nivel_8
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.municipio` AS dados
LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
    ON dados.id_municipio = diretorio_id_municipio.id_municipio
LEFT JOIN `dicionario_serie`
    ON dados.serie = chave_serie
LEFT JOIN `dicionario_rede`
    ON dados.rede = chave_rede
"""

bd.read_sql(query = query, billing_project_id = billing_id)


import basedosdados as bd

billing_id = <seu_billing_id>

query = """
  WITH 
dicionario_serie AS (
    SELECT
        chave AS chave_serie,
        valor AS descricao_serie
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'serie'
        AND id_tabela = 'alunos'
),
dicionario_rede AS (
    SELECT
        chave AS chave_rede,
        valor AS descricao_rede
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'rede'
        AND id_tabela = 'alunos'
),
dicionario_presenca AS (
    SELECT
        chave AS chave_presenca,
        valor AS descricao_presenca
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'presenca'
        AND id_tabela = 'alunos'
),
dicionario_preenchimento_caderno AS (
    SELECT
        chave AS chave_preenchimento_caderno,
        valor AS descricao_preenchimento_caderno
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'preenchimento_caderno'
        AND id_tabela = 'alunos'
),
dicionario_alfabetizado AS (
    SELECT
        chave AS chave_alfabetizado,
        valor AS descricao_alfabetizado
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'alfabetizado'
        AND id_tabela = 'alunos'
)
SELECT
    dados.ano as ano,
    dados.id_municipio AS id_municipio,
    diretorio_id_municipio.nome AS id_municipio_nome,
    dados.id_escola as id_escola,
    dados.id_aluno as id_aluno,
    dados.caderno as caderno,
    descricao_serie AS serie,
    descricao_rede AS rede,
    descricao_presenca AS presenca,
    descricao_preenchimento_caderno AS preenchimento_caderno,
    descricao_alfabetizado AS alfabetizado,
    dados.proficiencia as proficiencia,
    dados.peso_aluno as peso_aluno
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.alunos` AS dados
LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
    ON dados.id_municipio = diretorio_id_municipio.id_municipio
LEFT JOIN `dicionario_serie`
    ON dados.serie = chave_serie
LEFT JOIN `dicionario_rede`
    ON dados.rede = chave_rede
LEFT JOIN `dicionario_presenca`
    ON dados.presenca = chave_presenca
LEFT JOIN `dicionario_preenchimento_caderno`
    ON dados.preenchimento_caderno = chave_preenchimento_caderno
LEFT JOIN `dicionario_alfabetizado`
    ON dados.alfabetizado = chave_alfabetizado
"""

bd.read_sql(query = query, billing_project_id = billing_id)