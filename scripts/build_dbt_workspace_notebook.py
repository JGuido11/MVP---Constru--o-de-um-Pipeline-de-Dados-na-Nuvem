"""Empacota apenas o projeto dbt em um notebook instalador, sem credenciais ou logs."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DBT_ROOT = ROOT / "dbt"
DESTINATION = ROOT / "notebooks" / "05_install_dbt_project.py"

HEADER = '''# Databricks notebook source
# Gerado por scripts/build_dbt_workspace_notebook.py a partir do projeto dbt local.
# DBTITLE 1,Destino dos arquivos dbt no Workspace
dbutils.widgets.text("project_root", "/Workspace/Users/joaopgher@gmail.com/urban_mobility")
dbutils.widgets.dropdown("overwrite", "false", ["false", "true"], "Substituir arquivos diferentes?")

# COMMAND ----------

# DBTITLE 1,Arquivos do projeto dbt (SQL e YAML)
'''

FOOTER = r'''

# COMMAND ----------

# DBTITLE 1,Instalar os arquivos no Workspace
from pathlib import Path

project_root = Path(dbutils.widgets.get("project_root").strip())
if not str(project_root).startswith("/Workspace/") or ".." in project_root.parts:
    raise ValueError("project_root deve ser uma pasta do projeto dentro de /Workspace/.")
destination = project_root / "dbt"
overwrite = dbutils.widgets.get("overwrite") == "true"

# A tarefa dbt nativa gera o perfil a partir do SQL Warehouse selecionado.
# O perfil usado pelo runner local não deve ser instalado por este notebook.
if (destination / "profiles.yml").exists():
    raise ValueError(
        "Já existe dbt/profiles.yml. Renomeie esse perfil local para profiles.yml.local "
        "antes de usar a configuração automática do SQL Warehouse."
    )

conflicts = []
for relative, content in PROJECT_FILES.items():
    relative_path = Path(relative)
    if relative_path.is_absolute() or ".." in relative_path.parts:
        raise ValueError(f"Caminho inválido no pacote: {relative}")
    path = destination / relative_path
    if path.exists() and path.read_text(encoding="utf-8") != content:
        conflicts.append(relative)
if conflicts and not overwrite:
    raise ValueError(
        "Existem arquivos diferentes: " + ", ".join(conflicts)
        + ". Confira suas alterações; para substituir, ajuste overwrite=true."
    )

written = 0
for relative, content in PROJECT_FILES.items():
    path = destination / relative
    if not path.exists() or path.read_text(encoding="utf-8") != content:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        written += 1

print(f"Projeto dbt: {destination}")
print(f"Arquivos no pacote: {len(PROJECT_FILES)}; escritos nesta execução: {written}")
print("\n".join(sorted(PROJECT_FILES)))
print("Próximo passo: criar uma tarefa dbt com Source=Workspace e selecionar esta pasta.")
'''


def project_files():
    paths = [DBT_ROOT / "dbt_project.yml"]
    for folder in ("models", "macros", "tests", "analyses"):
        paths.extend(path for path in (DBT_ROOT / folder).rglob("*")
                     if path.suffix in {".sql", ".yml", ".yaml"})
    return {path.relative_to(DBT_ROOT).as_posix(): path.read_text(encoding="utf-8")
            for path in sorted(paths)}


def main():
    files = project_files()
    # json.dumps produces valid Python here because every key/value is a string.
    code = HEADER + "PROJECT_FILES = " + json.dumps(files, indent=4, ensure_ascii=False) + FOOTER
    compile(code, str(DESTINATION), "exec")
    DESTINATION.write_text(code, encoding="utf-8")
    print(f"Gerado: {DESTINATION.name} ({len(files)} arquivos dbt; sem profiles.yml)")


if __name__ == "__main__":
    main()
