"""Stub de Streamlit para correr mayoristas_streamlit_app.py fuera de la app (DRY-RUN).

SOLO LECTURA: no toca Dropbox salvo los files_download que el propio módulo hace.
Captura los mensajes de UI (info/warning/error) para poder reportarlos.
"""
import sys, types, pathlib, functools

APP_DIR = pathlib.Path("/Users/julianlopez/Library/CloudStorage/OneDrive-Personal/Encargomio/Mayoristas_app")
MENSAJES = []          # [(nivel, texto)]


class _Stop(Exception):
    """Equivalente a st.stop() fuera de Streamlit."""


class _Secrets(dict):
    def __getitem__(self, k):
        return super().__getitem__(k)


def _mk_st():
    st = types.ModuleType("streamlit")

    import toml
    st.secrets = _Secrets(toml.load(open(APP_DIR / ".streamlit" / "secrets.toml")))

    def _cap(nivel):
        def f(msg=None, *a, **k):
            if msg is not None:
                MENSAJES.append((nivel, str(msg)))
        return f

    for n in ("info", "warning", "error", "success", "caption", "write", "markdown",
              "header", "subheader", "title", "text", "dataframe", "table", "json",
              "metric", "code", "divider", "set_page_config", "toast", "badge"):
        setattr(st, n, _cap(n))

    def _stop(*a, **k):
        raise _Stop("st.stop() llamado")
    st.stop = _stop

    def cache_data(*a, **k):
        # soporta @st.cache_data y @st.cache_data(ttl=...)
        if len(a) == 1 and callable(a[0]) and not k:
            return a[0]
        return lambda f: f
    st.cache_data = cache_data
    st.cache_resource = cache_data

    st.session_state = {}
    st.file_uploader = lambda *a, **k: None
    st.radio = lambda *a, **k: None
    st.selectbox = lambda *a, **k: None
    st.button = lambda *a, **k: False
    st.checkbox = lambda *a, **k: False
    st.text_input = lambda *a, **k: ""
    st.columns = lambda n, *a, **k: [types.SimpleNamespace() for _ in range(n if isinstance(n, int) else len(n))]
    st.tabs = lambda names: [types.SimpleNamespace(__enter__=lambda s: s, __exit__=lambda *x: None)
                             for _ in names]
    st.expander = lambda *a, **k: types.SimpleNamespace(__enter__=lambda: None, __exit__=lambda *x: None)
    st.spinner = lambda *a, **k: types.SimpleNamespace(__enter__=lambda: None, __exit__=lambda *x: None)
    st.form = st.expander
    st.sidebar = types.SimpleNamespace(**{n: _cap(n) for n in ("info", "warning", "write")})
    st.rerun = lambda *a, **k: None
    st.download_button = lambda *a, **k: False
    return st


def cargar_app():
    """Importa el módulo de la app con streamlit stubeado. Devuelve el módulo."""
    if "streamlit" not in sys.modules:
        sys.modules["streamlit"] = _mk_st()
    if str(APP_DIR) not in sys.path:
        sys.path.insert(0, str(APP_DIR))
    import importlib
    mod = importlib.import_module("mayoristas_streamlit_app")
    return mod


def drenar():
    """Devuelve y limpia los mensajes capturados."""
    out = list(MENSAJES)
    MENSAJES.clear()
    return out


def clear_msgs():
    """Limpia el buffer de mensajes (compatibilidad con los scripts de carga)."""
    MENSAJES.clear()
