-- Make the structure of Electrons and Muons uniform, and then union their arrays
WITH uniform_structure_leptons AS (
    SELECT
        event,
        MET_pt,
        MET_phi,
        array_union(
            transform(
                COALESCE(Muons, ARRAY []),
                x -> CAST( ROW(x.pt, x.eta, x.phi, x.mass, x.charge, 'm') AS ROW( pt DOUBLE, eta DOUBLE, phi DOUBLE, mass DOUBLE, charge INTEGER, type CHAR ) )
            ),
            transform(
                COALESCE(Electrons, ARRAY []),
                x -> CAST( ROW(x.pt, x.eta, x.phi, x.mass, x.charge, 'e') AS ROW( pt DOUBLE, eta DOUBLE, phi DOUBLE, mass DOUBLE, charge INTEGER, type CHAR ) )
            )
        ) AS Leptons
    FROM {input_table}
    WHERE nMuon + nElectron > 2
),


-- Create the Lepton pairs, transform the leptons using PtEtaPhiM2PxPyPzE and then sum the transformed leptons
lepton_pairs AS (
    SELECT
        *,
        CAST(
            ROW(
                pt1 * cos(phi1) + pt2 * cos(phi2),
                pt1 * sin(phi1) + pt2 * sin(phi2),
                pt1 * ( ( exp(eta1) - exp(-eta1) ) / 2.0 ) + pt2 * ( ( exp(eta2) - exp(-eta2) ) / 2.0 ),
                pt1 * cosh(eta1) * pt1 * cosh(eta1) * pt1 + mass1 * mass1 + pt2 * cosh(eta2) * pt2 * cosh(eta2) * pt2 + mass2 * mass2
            ) AS
            ROW (x DOUBLE, y DOUBLE, z DOUBLE, e DOUBLE)
        ) AS l,
        idx1 AS l1_idx,
        idx2 AS l2_idx
    FROM uniform_structure_leptons
    CROSS JOIN UNNEST(Leptons) WITH ORDINALITY AS l1 (pt1, eta1, phi1, mass1, charge1, type1, idx1)
    CROSS JOIN UNNEST(Leptons) WITH ORDINALITY AS l2 (pt2, eta2, phi2, mass2, charge2, type2, idx2)
    WHERE idx1 < idx2 AND type1 = type2 AND charge1 != charge2
),


-- Apply the PtEtaPhiM2PxPyPzE transformation on the particle pairs, then retrieve the one with the mass closest to 91.2 for each event
processed_pairs AS (
    SELECT
        event,
        min_by(
            ROW(
                l1_idx,
                l2_idx,
                Leptons,
                MET_pt,
                MET_phi
            ),
            abs(91.2 - sqrt(l.e * l.e - l.x * l.x - l.y * l.y - l.z * l.z))
        ) AS system
    FROM lepton_pairs
    GROUP BY event
),


-- For each event get the max pt of the other leptons
other_max_pt AS (
    SELECT event, max(pt) AS pt
    FROM processed_pairs
    CROSS JOIN UNNEST(system[3]) WITH ORDINALITY AS l (pt, eta, phi, mass, charge, type, idx)
    WHERE idx != system[1] AND idx != system[2]
    GROUP BY event
)


-- Compute the histogram
SELECT
  CAST((
    CASE
      WHEN pt < 15 THEN 15
      WHEN pt > 60 THEN 60
      ELSE pt
    END - 0.225) / 0.45 AS BIGINT) * 0.45 + 0.225 AS x,
  COUNT(*) AS y
  FROM other_max_pt
  GROUP BY CAST((
    CASE
      WHEN pt < 15 THEN 15
      WHEN pt > 60 THEN 60
      ELSE pt
    END - 0.225) / 0.45 AS BIGINT) * 0.45 + 0.225
  ORDER BY x;
