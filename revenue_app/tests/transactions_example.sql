SELECT
column_one,
column_two,
sum(column_three/100.00) AS column_three
FROM some_table
JOIN other_table o ON o.id = column_one
WHERE column_date >= CAST('2019-12-10' AS DATE)
AND column_date <= CAST('2019-12-25' AS DATE)
AND column_three IN ('case1', 'case2')
GROUP BY 1, 2
ORDER BY 1, 2
