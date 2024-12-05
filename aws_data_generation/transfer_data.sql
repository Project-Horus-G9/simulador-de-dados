USE horus_dimensional;

INSERT INTO dim_empresa (nome)
SELECT DISTINCT nome
FROM horus_trusted.empresa emp
WHERE nome IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM dim_empresa de
      WHERE de.nome = emp.nome
  );

INSERT INTO dim_estado (estado)
SELECT DISTINCT e.estado
FROM horus_trusted.endereco e
WHERE estado IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM dim_estado ds
      WHERE ds.estado = e.estado
  );

INSERT INTO dim_cidade (cidade, fk_estado)
SELECT DISTINCT e.cidade, ds.id_estado
FROM horus_trusted.endereco e
JOIN dim_estado ds ON ds.estado = e.estado
WHERE cidade IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM dim_cidade dc
      WHERE dc.cidade = e.cidade
        AND dc.fk_estado = ds.id_estado
  );

INSERT INTO dim_setor (nome, fk_empresa)
SELECT DISTINCT s.nome, de.id_empresa
FROM horus_trusted.setor s
JOIN horus_trusted.empresa e ON s.fk_empresa = e.id_empresa
JOIN dim_empresa de ON de.nome = e.nome
WHERE s.nome IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM dim_setor ds
      WHERE ds.nome = s.nome
        AND ds.fk_empresa = de.id_empresa
  );

INSERT INTO dim_painel (nome, fk_setor)
SELECT DISTINCT p.nome, ds.id_setor
FROM horus_trusted.painel p
JOIN horus_trusted.setor s ON p.fk_setor = s.id_setor
JOIN dim_setor ds ON ds.nome = s.nome
WHERE p.nome IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM dim_painel dp
      WHERE dp.nome = p.nome
        AND dp.fk_setor = ds.id_setor
  );

INSERT INTO dim_tempo (horario, dia, mes, ano)
SELECT DISTINCT TIME(dl.data), DAY(dl.data), MONTH(dl.data), YEAR(dl.data)
FROM horus_trusted.dados_leitura dl
WHERE dl.data IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM dim_tempo dt
      WHERE dt.horario = TIME(dl.data)
        AND dt.dia = DAY(dl.data)
        AND dt.mes = MONTH(dl.data)
        AND dt.ano = YEAR(dl.data)
  );

INSERT INTO dim_umidade (umidade)
SELECT DISTINCT umidade
FROM horus_trusted.dados_leitura dl
WHERE umidade IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM dim_umidade du
      WHERE du.umidade = dl.umidade
  );

INSERT INTO dim_tensao (tensao)
SELECT DISTINCT tensao
FROM horus_trusted.dados_leitura dl
WHERE tensao IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM dim_tensao dt
      WHERE dt.tensao = dl.tensao
  );

INSERT INTO dim_orientacao (direcionamento, inclinacao)
SELECT DISTINCT direcionamento, inclinacao
FROM horus_trusted.dados_leitura dl
WHERE direcionamento IS NOT NULL
  AND inclinacao IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM dim_orientacao dor
      WHERE dor.direcionamento = dl.direcionamento
        AND dor.inclinacao = dl.inclinacao
  );

INSERT INTO fato_dados (
    id_tempo,
    id_painel,
    id_umidade,
    id_tensao,
    id_orientacao,
    m_temp_interna,
    m_temp_externa,
    m_energia_esperada,
    m_energia_gerada,
    m_luminosidade,
    m_eficiencia,
    m_obstrucao,
    m_ceu
)
SELECT
    dt.id_tempo,
    dp.id_painel,
    du.id_umidade,
    dtens.id_tensao,
    dor.id_orientacao,
    dl.temperatura_interna,
    dl.temperatura_externa,
    dl.energia_esperada,
    dl.energia_gerada,
    dl.luminosidade,
    dl.eficiencia,
    dl.obstrucao,
    dl.ceu
FROM horus_trusted.dados_leitura dl
JOIN dim_tempo dt ON dt.horario = TIME(dl.data)
                 AND dt.dia = DAY(dl.data)
                 AND dt.mes = MONTH(dl.data)
                 AND dt.ano = YEAR(dl.data)
JOIN dim_painel dp ON dp.nome = (SELECT nome FROM horus_trusted.painel WHERE id_painel = dl.fk_painel)
JOIN dim_umidade du ON du.umidade = dl.umidade
JOIN dim_tensao dtens ON dtens.tensao = dl.tensao
JOIN dim_orientacao dor ON dor.direcionamento = dl.direcionamento
                        AND dor.inclinacao = dl.inclinacao
WHERE dl.temperatura_interna IS NOT NULL;
