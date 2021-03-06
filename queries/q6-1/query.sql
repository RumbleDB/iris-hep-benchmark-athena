-- Create the TriJet systems
WITH tri_jets AS (
  SELECT event, m1, m2, m3
  FROM {input_table}
  CROSS JOIN UNNEST(Jet) WITH ORDINALITY AS _m1(m1, idx1)
  CROSS JOIN UNNEST(Jet) WITH ORDINALITY AS _m2(m2, idx2)
  CROSS JOIN UNNEST(Jet) WITH ORDINALITY AS _m2(m3, idx3)
  WHERE idx1 < idx2 AND idx2 < idx3
),


-- Compute the PtEtaPhiM2PxPyPzE for each particle
expanded_tri_jet AS (
  SELECT
    event,
    CAST(
      ROW(
        m1.pt * cos(m1.phi),
        m1.pt * sin(m1.phi),
        m1.pt * ( ( exp(m1.eta) - exp(-m1.eta) ) / 2.0 ),
        m1.pt * cosh(m1.eta) * m1.pt * cosh(m1.eta) * m1.pt + m1.mass * m1.mass
      ) AS
      ROW (x REAL, y REAL, z REAL, e REAL)
    ) AS m1,
    CAST(
      ROW(
        m2.pt * cos(m2.phi),
        m2.pt * sin(m2.phi),
        m2.pt * ( ( exp(m2.eta) - exp(-m2.eta) ) / 2.0 ),
        m2.pt * cosh(m2.eta) * m2.pt * cosh(m2.eta) * m2.pt + m2.mass * m2.mass
      ) AS
      ROW (x REAL, y REAL, z REAL, e REAL)
    ) AS m2,
    CAST(
      ROW(
        m3.pt * cos(m3.phi),
        m3.pt * sin(m3.phi),
        m3.pt * ( ( exp(m3.eta) - exp(-m3.eta) ) / 2.0 ),
        m3.pt * cosh(m3.eta) * m3.pt * cosh(m3.eta) * m3.pt + m3.mass * m3.mass
      ) AS
      ROW (x REAL, y REAL, z REAL, e REAL)
    ) AS m3,
    m1 AS m1_particle,
    m2 AS m2_particle,
    m3 AS m3_particle
  FROM tri_jets
),


-- Compute the AddPxPyPzE3 for each TriJet system
condensed_tri_jet AS (
  SELECT
    event,
    m1.x + m2.x + m3.x AS x,
    m1.y + m2.y + m3.y AS y,
    m1.z + m2.z + m3.z AS z,
    m1.e + m2.e + m3.e AS e,
    (m1.x + m2.x + m3.x) * (m1.x + m2.x + m3.x) AS x2,
    (m1.y + m2.y + m3.y) * (m1.y + m2.y + m3.y) AS y2,
    (m1.z + m2.z + m3.z) * (m1.z + m2.z + m3.z) AS z2,
    (m1.e + m2.e + m3.e) * (m1.e + m2.e + m3.e) AS e2,
    m1_particle AS m1,
    m2_particle AS m2,
    m3_particle AS m3
  FROM expanded_tri_jet
),


-- Compute the PxPyPzE2PtEtaPhiM
computed_system AS (
  SELECT
    event,
    sqrt(x2 * y2) AS pt,
    ln( (z / sqrt(x2 * y2)) + sqrt((z / sqrt(x2 * y2)) * (z / sqrt(x2 * y2)) + 1.0)) AS eta,
    CASE
      WHEN x = 0 AND y = 0 THEN 0.0
      ELSE atan2(y, x)
    END AS phi,
    sqrt(e2 - x2 - y2 - z2) AS mass,
    m1,
    m2,
    m3
  FROM condensed_tri_jet
),


-- Find the system with the lowest mass
singular_system AS (
  SELECT
    event,
    min_by(
      ARRAY [m1, m2, m3],
      abs(172.5 - mass)
    ) AS jet_system
  FROM computed_system
  GROUP BY event
)


-- Generate the histogram
SELECT
  CAST((
    CASE
      WHEN jet.pt < 15 THEN 15
      WHEN jet.pt > 40 THEN 40
      ELSE jet.pt
    END - 0.125) / 0.25 AS BIGINT) * 0.25 + 0.125 AS x,
  COUNT(*) AS y
FROM singular_system
CROSS JOIN UNNEST(jet_system) AS _jet(jet)
GROUP BY CAST((
    CASE
      WHEN jet.pt < 15 THEN 15
      WHEN jet.pt > 40 THEN 40
      ELSE jet.pt
    END - 0.125) / 0.25 AS BIGINT) * 0.25 + 0.125
ORDER BY x;
