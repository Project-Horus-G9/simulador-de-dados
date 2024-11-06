USE horus_dimensional;

INSERT INTO horus_dimensional.dim_empresa (nome)
SELECT nome
FROM (
    SELECT DISTINCT nome
    FROM horus_trusted.empresa
    WHERE nome IS NOT NULL
) AS empresas
WHERE NOT EXISTS (
    SELECT 1
    FROM horus_dimensional.dim_empresa de
    WHERE de.nome = empresas.nome
);

INSERT INTO horus_dimensional.estado (estado, fk_empresa)
SELECT DISTINCT e.estado, de.id_empresa
FROM horus_trusted.endereco e
JOIN horus_trusted.empresa emp ON emp.id_empresa = e.fk_empresa
JOIN horus_dimensional.dim_empresa de ON de.nome = emp.nome
WHERE e.estado IS NOT NULL
AND NOT EXISTS (
    SELECT 1
    FROM horus_dimensional.estado est
    WHERE est.estado = e.estado
      AND est.fk_empresa = de.id_empresa
);

INSERT INTO horus_dimensional.cidade (cidade, fk_estado)
SELECT DISTINCT e.cidade, est.id_estado
FROM horus_trusted.endereco e
JOIN horus_trusted.empresa emp ON emp.id_empresa = e.fk_empresa
JOIN horus_dimensional.dim_empresa de ON de.nome = emp.nome
JOIN horus_dimensional.estado est ON est.estado = e.estado AND est.fk_empresa = de.id_empresa
WHERE e.cidade IS NOT NULL
AND NOT EXISTS (
    SELECT 1
    FROM horus_dimensional.cidade ci
    WHERE ci.cidade = e.cidade
      AND ci.fk_estado = est.id_estado
);

INSERT INTO horus_dimensional.dim_setor (nome)
SELECT nome
FROM (
    SELECT DISTINCT nome
    FROM horus_trusted.setor
    WHERE nome IS NOT NULL
) AS setores
WHERE NOT EXISTS (
    SELECT 1
    FROM horus_dimensional.dim_setor ds
    WHERE ds.nome = setores.nome
);

INSERT INTO horus_dimensional.dim_painel (nome)
SELECT nome
FROM (
    SELECT DISTINCT nome
    FROM horus_trusted.painel
    WHERE nome IS NOT NULL
) AS paineis
WHERE NOT EXISTS (
    SELECT 1
    FROM horus_dimensional.dim_painel dp
    WHERE dp.nome = paineis.nome
);

INSERT INTO horus_dimensional.dim_tempo (horario, dia, mes, ano)
SELECT horario, dia, mes, ano
FROM (
    SELECT DISTINCT TIME(data) AS horario, DAY(data) AS dia, MONTH(data) AS mes, YEAR(data) AS ano
    FROM horus_trusted.dados_leitura
    WHERE data IS NOT NULL
) AS tempos
WHERE NOT EXISTS (
    SELECT 1
    FROM horus_dimensional.dim_tempo dt
    WHERE dt.horario = tempos.horario
      AND dt.dia = tempos.dia
      AND dt.mes = tempos.mes
      AND dt.ano = tempos.ano
);

INSERT INTO horus_dimensional.dim_umidade (umidade)
SELECT umidade
FROM (
    SELECT DISTINCT umidade
    FROM horus_trusted.dados_leitura
    WHERE umidade IS NOT NULL
) AS umidades
WHERE NOT EXISTS (
    SELECT 1
    FROM horus_dimensional.dim_umidade du
    WHERE du.umidade = umidades.umidade
);

INSERT INTO horus_dimensional.dim_obstrucao (obstrucao)
SELECT obstrucao
FROM (
    SELECT DISTINCT obstrucao
    FROM horus_trusted.dados_leitura
    WHERE obstrucao IS NOT NULL
) AS obstrucoes
WHERE NOT EXISTS (
    SELECT 1
    FROM horus_dimensional.dim_obstrucao dob
    WHERE dob.obstrucao = obstrucoes.obstrucao
);

INSERT INTO horus_dimensional.dim_ceu (ceu, luminosidade)
SELECT ceu, luminosidade
FROM (
    SELECT DISTINCT ceu, luminosidade
    FROM horus_trusted.dados_leitura
    WHERE ceu IS NOT NULL AND luminosidade IS NOT NULL
) AS ceus
WHERE NOT EXISTS (
    SELECT 1
    FROM horus_dimensional.dim_ceu dc
    WHERE dc.ceu = ceus.ceu
      AND dc.luminosidade = ceus.luminosidade
);

INSERT INTO horus_dimensional.dim_energia (energia_esperada, energia_gerada, eficiencia)
SELECT energia_esperada, energia_gerada, eficiencia
FROM (
    SELECT DISTINCT energia_esperada, energia_gerada, eficiencia
    FROM horus_trusted.dados_leitura
    WHERE energia_esperada IS NOT NULL
      AND energia_gerada IS NOT NULL
      AND eficiencia IS NOT NULL
) AS energias
WHERE NOT EXISTS (
    SELECT 1
    FROM horus_dimensional.dim_energia de
    WHERE de.energia_esperada = energias.energia_esperada
      AND de.energia_gerada = energias.energia_gerada
      AND de.eficiencia = energias.eficiencia
);

INSERT INTO horus_dimensional.dim_tensao (tensao)
SELECT tensao
FROM (
    SELECT DISTINCT tensao
    FROM horus_trusted.dados_leitura
    WHERE tensao IS NOT NULL
) AS tensoes
WHERE NOT EXISTS (
    SELECT 1
    FROM horus_dimensional.dim_tensao dt
    WHERE dt.tensao = tensoes.tensao
);

INSERT INTO horus_dimensional.dim_orientacao (direcionamento, inclinacao)
SELECT direcionamento, inclinacao
FROM (
    SELECT DISTINCT direcionamento, inclinacao
    FROM horus_trusted.dados_leitura
    WHERE direcionamento IS NOT NULL AND inclinacao IS NOT NULL
) AS orientacoes
WHERE NOT EXISTS (
    SELECT 1
    FROM horus_dimensional.dim_orientacao dor
    WHERE dor.direcionamento = orientacoes.direcionamento
      AND dor.inclinacao = orientacoes.inclinacao
);

INSERT INTO horus_dimensional.fato_dados (
    id_setor,
    id_painel,
    id_empresa,
    id_umidade,
    id_obstrucao,
    id_ceu,
    id_energia,
    id_tensao,
    id_orientacao,
    m_temp_interna,
    m_temp_externa
)
SELECT
    ds.id_setor,
    dp.id_painel,
    de.id_empresa,
    du.id_umidade,
    dobs.id_obstrucao,
    dc.id_ceu,
    den.id_energia,
    dt.id_tensao,
    dor.id_orientacao,
    dl.temperatura_interna,
    dl.temperatura_externa
FROM horus_trusted.dados_leitura dl
-- Associar com dimensões correspondentes
JOIN horus_trusted.painel p ON dl.fk_painel = p.id_painel
JOIN horus_trusted.setor s ON p.fk_setor = s.id_setor
JOIN horus_trusted.empresa e ON s.fk_empresa = e.id_empresa
-- Dimensões no horus_dimensional
JOIN horus_dimensional.dim_setor ds ON ds.nome = s.nome
JOIN horus_dimensional.dim_painel dp ON dp.nome = p.nome
JOIN horus_dimensional.dim_empresa de ON de.nome = e.nome
JOIN horus_dimensional.dim_umidade du ON du.umidade = dl.umidade
JOIN horus_dimensional.dim_obstrucao dobs ON dobs.obstrucao = dl.obstrucao
JOIN horus_dimensional.dim_ceu dc ON dc.ceu = dl.ceu AND dc.luminosidade = dl.luminosidade
JOIN horus_dimensional.dim_energia den ON den.energia_esperada = dl.energia_esperada AND den.energia_gerada = dl.energia_gerada AND den.eficiencia = dl.eficiencia
JOIN horus_dimensional.dim_tensao dt ON dt.tensao = dl.tensao
JOIN horus_dimensional.dim_orientacao dor ON dor.direcionamento = dl.direcionamento AND dor.inclinacao = dl.inclinacao;
