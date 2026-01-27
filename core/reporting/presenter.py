class HTMLPresenter:
    """
    Responsável pela formatação visual do relatório (HTML).
    Isola a camada de apresentação da lógica de negócios.
    """

    @staticmethod
    def build_narrative_html(df_climate):
        """
        Converte os dados climáticos brutos em blocos HTML formatados.
        """
        narrative_groups = {"production": [], "logistics": [], "global": []}
        
        if df_climate is None or df_climate.empty: 
            return narrative_groups

        # Ordenação Visual: Vermelho (1) -> Amarelo (2) -> Verde (3)
        df_sorted = df_climate.copy()
        df_sorted['sort_key'] = df_sorted['Risk_Status'].apply(
            lambda x: 1 if '🔴' in str(x) else (2 if '🟡' in str(x) else 3)
        )
        df_sorted = df_sorted.sort_values('sort_key')
        
        seen_tags = set()
        
        # Keywords de Categorização Visual
        kw_logistics = [
            'SANTOS', 'PARANAGUA', 'ITAQUI', 'RIO GRANDE', 'PORTO', 
            'BR-163', 'FERROVIA', 'BR-364', 'HIDROVIA', 'PANAMA', 'SUEZ', 
            'RHINE', 'ROTTERDAM'
        ]
        kw_production = [
            'MT', 'PR', 'GO', 'MS', 'SORRISO', 'SINOP', 'LUCAS', 
            'CASCAVEL', 'RIO VERDE', 'MATO', 'PARANA', 'BAHIA',
            'IOWA', 'ILLINOIS', 'DES MOINES', 'CORN BELT'
        ]

        for _, row in df_sorted.iterrows():
            status = str(row['Risk_Status'])
            
            # Filtro de Relevância Visual (Ignora Verde/Branco no texto)
            if '🟢' in status or '⚪' in status: continue

            loc = str(row['Location']).upper()
            if loc in seen_tags: continue
            seen_tags.add(loc)

            # Estilização
            icon = "🚨" if "🔴" in status else "⚠️"
            color = "#cf222e" if "🔴" in status else "#d29922"
            
            item_html = f"<li style='margin-bottom: 5px; color: #24292f;'><strong>{icon} {loc}:</strong> <span style='color: {color};'>{status}</span></li>"
            
            if any(k in loc for k in kw_logistics): 
                narrative_groups['logistics'].append(item_html)
            elif any(k in loc for k in kw_production): 
                narrative_groups['production'].append(item_html)
            else: 
                narrative_groups['global'].append(item_html)

        return narrative_groups