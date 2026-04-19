"""
Portuguese (pt-BR) translations for dataset and column names.

This module provides mappings for translating English dataset and column names
to Portuguese for use in Portuguese-language notebooks and reports.

Usage:
    from src.config.pt_br_translations import translate_column_name, translate_dataset_name
    
    # Translate a single column
    pt_name = translate_column_name('state_code')  # Returns 'codigo_estado'
    
    # Translate a dataset name
    pt_dataset = translate_dataset_name('municipality_socioeconomic')  # Returns 'municipio_socioeconomico'
    
    # Rename all columns in a DataFrame
    df_pt = df.rename(columns=COLUMN_TRANSLATIONS)
"""

# Dataset name translations
DATASET_TRANSLATIONS = {
    'municipality_socioeconomic': 'municipio_socioeconomico',
    'state_summary': 'resumo_estado',
    'sanctions_summary': 'resumo_sancoes',
    'analysis_compliance': 'analise_compliance',
    'analysis_compliance_municipality': 'analise_compliance_municipio',
    'consolidated_clustering': 'clustering_consolidado'
}

# Column name translations
COLUMN_TRANSLATIONS = {
    # Municipality identifiers
    'municipality_code': 'codigo_municipio',
    'municipality_name': 'nome_municipio',
    'state_code': 'codigo_estado',
    'state_name': 'nome_estado',
    'region_code': 'codigo_regiao',
    'region_name': 'nome_regiao',
    
    # Population metrics
    'population': 'populacao',
    'population_2010': 'populacao_2010',
    'population_2022': 'populacao_2022',
    'population_change_pct': 'mudanca_populacao_pct',
    'log_population': 'log_populacao',
    
    # Literacy metrics
    'literacy_rate': 'taxa_alfabetizacao',
    'literacy_rate_2010': 'taxa_alfabetizacao_2010',
    'literacy_rate_2022': 'taxa_alfabetizacao_2022',
    'literacy_change_pp': 'mudanca_alfabetizacao_pp',
    'avg_literacy_rate': 'taxa_alfabetizacao_media',
    
    # Income metrics
    'avg_income': 'renda_media',
    'avg_income_2010': 'renda_media_2010',
    'avg_income_2022': 'renda_media_2022',
    'avg_income_real_2010_2022_brl': 'renda_media_real_2010_brl_2022',
    'avg_income_real_2022_2022_brl': 'renda_media_real_2022_brl_2022',
    'income_change_pct': 'mudanca_renda_pct',
    'income_change_real_pct': 'mudanca_renda_real_pct',
    'annual_ipca_rate_pct': 'ipca_anual_pct',
    'ipca_index_avg': 'indice_ipca_medio',
    'deflator_to_2022': 'deflator_para_2022',
    'avg_income_real_2022_brl': 'renda_media_real_brl_2022',
    'log_income': 'log_renda',
    
    # Household metrics
    'total_households': 'total_domicilios',
    'households_2010': 'domicilios_2010',
    'households_2022': 'domicilios_2022',
    'households_change_pct': 'mudanca_domicilios_pct',
    
    # Sanctions metrics
    'n_sanctions': 'num_sancoes',
    'n_sanctions_ceis': 'num_sancoes_ceis',
    'n_sanctions_cnep': 'num_sancoes_cnep',
    'n_sanctions_cepim': 'num_sancoes_cepim',
    'sanctions_per_100k': 'sancoes_por_100k',
    'sanctions_per_million_brl_transfers': 'sancoes_por_milhao_brl_transferencias',
    'total_sanctions': 'total_sancoes',
    
    # Sanctions by type
    'sanctions_pf': 'sancoes_pf',
    'sanctions_pj': 'sancoes_pj',
    'pj_ratio_pct': 'razao_pj_pct',
    
    # Registry types
    'registry_type': 'tipo_registro',
    
    # Aggregation metrics
    'n_municipalities': 'num_municipios',
    
    # Regional dummy variables
    'is_norte': 'eh_norte',
    'is_nordeste': 'eh_nordeste',
    'is_sudeste': 'eh_sudeste',
    'is_sul': 'eh_sul',
    'is_centro_oeste': 'eh_centro_oeste',
    
    # Federal transfers
    'total_transfers': 'total_transferencias',
    'n_transfer_records': 'num_registros_transferencia',
    'avg_transfer_per_capita': 'transferencia_media_per_capita',
    'log_total_transfers': 'log_total_transferencias',
}

# Reverse mappings (Portuguese to English)
DATASET_TRANSLATIONS_REVERSE = {v: k for k, v in DATASET_TRANSLATIONS.items()}
COLUMN_TRANSLATIONS_REVERSE = {v: k for k, v in COLUMN_TRANSLATIONS.items()}

# Display name translations (for charts and reports)
DISPLAY_NAME_TRANSLATIONS = {
    # Dataset display names
    'municipality_socioeconomic': 'Município - Socioeconômico',
    'state_summary': 'Resumo por Estado',
    'sanctions_summary': 'Resumo de Sanções',
    'analysis_compliance': 'Análise de Compliance',
    'analysis_compliance_municipality': 'Análise de Compliance Municipal',
    'consolidated_clustering': 'Clustering Consolidado',
    
    # Column display names
    'state_code': 'Código do Estado',
    'state_name': 'Nome do Estado',
    'region_code': 'Código da Região',
    'region_name': 'Nome da Região',
    'municipality_code': 'Código do Município',
    'municipality_name': 'Nome do Município',
    
    'population': 'População',
    'population_2010': 'População (2010)',
    'population_2022': 'População (2022)',
    'population_change_pct': 'Mudança Populacional (%)',
    'log_population': 'Log(População)',
    
    'literacy_rate': 'Taxa de Alfabetização',
    'literacy_rate_2010': 'Taxa de Alfabetização (2010)',
    'literacy_rate_2022': 'Taxa de Alfabetização (2022)',
    'literacy_change_pp': 'Mudança na Alfabetização (p.p.)',
    'avg_literacy_rate': 'Taxa Média de Alfabetização',
    
    'avg_income': 'Renda Média',
    'avg_income_2010': 'Renda Média (2010)',
    'avg_income_2022': 'Renda Média (2022)',
    'avg_income_real_2010_2022_brl': 'Renda Média Real de 2010 em BRL de 2022',
    'avg_income_real_2022_2022_brl': 'Renda Média Real de 2022 em BRL de 2022',
    'income_change_pct': 'Mudança na Renda (%)',
    'income_change_real_pct': 'Mudança na Renda Real (%)',
    'annual_ipca_rate_pct': 'IPCA Anual (%)',
    'ipca_index_avg': 'Índice IPCA Médio',
    'deflator_to_2022': 'Deflator para 2022',
    'avg_income_real_2022_brl': 'Renda Média Real em BRL de 2022',
    'log_income': 'Log(Renda)',
    
    'total_households': 'Total de Domicílios',
    'households_2010': 'Domicílios (2010)',
    'households_2022': 'Domicílios (2022)',
    'households_change_pct': 'Mudança em Domicílios (%)',
    
    'n_sanctions': 'Número de Sanções',
    'n_sanctions_ceis': 'Sanções CEIS',
    'n_sanctions_cnep': 'Sanções CNEP',
    'n_sanctions_cepim': 'Sanções CEPIM',
    'sanctions_per_100k': 'Sanções por 100 mil hab.',
    'sanctions_per_million_brl_transfers': 'Sanções por Milhão de BRL em Transferências',
    'total_sanctions': 'Total de Sanções',
    
    'sanctions_pf': 'Sanções - Pessoa Física',
    'sanctions_pj': 'Sanções - Pessoa Jurídica',
    'pj_ratio_pct': 'Proporção PJ (%)',
    
    'registry_type': 'Tipo de Registro',
    'n_municipalities': 'Número de Municípios',
    
    'is_norte': 'Região Norte',
    'is_nordeste': 'Região Nordeste',
    'is_sudeste': 'Região Sudeste',
    'is_sul': 'Região Sul',
    'is_centro_oeste': 'Região Centro-Oeste',
    'total_transfers': 'Total de Transferências',
    'n_transfer_records': 'Número de Registros de Transferência',
    'avg_transfer_per_capita': 'Transferência Média per Capita',
    'log_total_transfers': 'Log(Total de Transferências)',
}


def translate_column_name(column_name: str, display: bool = False) -> str:
    """
    Translate an English column name to Portuguese.
    
    Args:
        column_name: English column name
        display: If True, return display name; if False, return code-friendly name
        
    Returns:
        Portuguese translation of the column name
    """
    if display:
        return DISPLAY_NAME_TRANSLATIONS.get(column_name, column_name)
    return COLUMN_TRANSLATIONS.get(column_name, column_name)


def translate_dataset_name(dataset_name: str, display: bool = False) -> str:
    """
    Translate an English dataset name to Portuguese.
    
    Args:
        dataset_name: English dataset name
        display: If True, return display name; if False, return code-friendly name
        
    Returns:
        Portuguese translation of the dataset name
    """
    if display:
        return DISPLAY_NAME_TRANSLATIONS.get(dataset_name, dataset_name)
    return DATASET_TRANSLATIONS.get(dataset_name, dataset_name)


def translate_dataframe_columns(df, display: bool = False):
    """
    Translate all column names in a DataFrame to Portuguese.
    
    Args:
        df: pandas DataFrame
        display: If True, use display names; if False, use code-friendly names
        
    Returns:
        DataFrame with translated column names
    """
    import pandas as pd
    
    translation_dict = DISPLAY_NAME_TRANSLATIONS if display else COLUMN_TRANSLATIONS
    return df.rename(columns=translation_dict)


def get_translation_summary() -> str:
    """
    Get a summary of available translations.
    
    Returns:
        Formatted string with translation counts
    """
    return f"""
Portuguese Translation Mappings Summary
========================================
Dataset translations: {len(DATASET_TRANSLATIONS)}
Column translations: {len(COLUMN_TRANSLATIONS)}
Display name translations: {len(DISPLAY_NAME_TRANSLATIONS)}

Available datasets:
{chr(10).join(f"  • {k} → {v}" for k, v in DATASET_TRANSLATIONS.items())}
"""


if __name__ == "__main__":
    print(get_translation_summary())
