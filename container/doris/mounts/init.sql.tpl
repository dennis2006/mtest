SET PASSWORD = PASSWORD('{{ .Password }}');
GO

CREATE DATABASE IF NOT EXISTS {{ .Database }};
USE {{ .Database }};
GO
