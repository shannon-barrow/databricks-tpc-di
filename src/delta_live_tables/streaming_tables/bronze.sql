CREATE OR REFRESH STREAMING TABLE {{catalog}}.{{database}}.{{table_name}} ({{schema}}) AS SELECT *
FROM STREAM read_files(
  {{ files_directory }}, --/tmp/tpcdi/sf=10/
  format => 'csv',
  header => {{header}}, --True
  inferSchema => False,
  sep => {{ sep }}, --,
  fileNamePattern => {{ filename }}, --*_audit.csv
  schema => "{{schema}}"
);