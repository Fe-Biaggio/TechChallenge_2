"""Configuração compartilhada dos testes — garante que o root do projeto está no path
e expõe um helper para importar os scripts numerados (01_, 02_, 03_...), que não são
importáveis com a sintaxe normal de import por começarem com dígito."""
import importlib.util
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def importar_modulo(caminho: str, nome: str):
    """Importa um módulo Python a partir de um caminho de arquivo (uso: scripts 01_/02_/03_)."""
    spec = importlib.util.spec_from_file_location(nome, ROOT_DIR / caminho)
    modulo = importlib.util.module_from_spec(spec)
    sys.modules[nome] = modulo
    spec.loader.exec_module(modulo)
    return modulo
