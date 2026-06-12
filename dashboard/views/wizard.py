"""
dashboard/views/wizard.py
=========================
Wizard — interactive DB operations.

Split out of app.py so st.navigation only executes the selected page
(the old st.tabs layout ran every tab's queries on every rerun).
"""
from __future__ import annotations

import streamlit as st

from dashboard import wizard_view


def render() -> None:
    wizard_view.render()
