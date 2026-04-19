"""
Transparency Portal Data Transformer for Silver Layer

Transforms Transparency Portal data from Bronze to Silver layer:
- Federal Transfers (2010-2022, monthly files)
- Compliance Sanctions (CEIS, CNEP, CEAF, CEPIM)

Features:
- Idempotent: Safe to run multiple times
- Graceful: Skips missing files without failing
- Dynamic: Discovers monthly files automatically
"""

import io
import logging
import re
import unicodedata
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime

import pandas as pd
from botocore.exceptions import ClientError

from src.processing.base_transformer import BaseTransformer

logger = logging.getLogger(__name__)


class TransparencyTransformer(BaseTransformer):
    """Transformer for Transparency Portal data (Bronze II → Silver)."""

    # Compliance sanctions - static file paths
    SANCTIONS_FILES = {
        'ceis_sanctions': 'bronze/transparency/ceis_compliance.json',
        'cnep_sanctions': 'bronze/transparency/cnep_compliance.json',
        # CEAF excluded - no location data available, file deleted from bronze
        'cepim_sanctions': 'bronze/transparency/cepim_compliance.json',
    }
    
    # Federal transfers use monthly file pattern: federal_transfers_YYYY_MM.json
    # Files are discovered dynamically from S3

    def get_source_datasets(self) -> List[str]:
        """Return list of source dataset names."""
        return ['federal_transfers'] + list(self.SANCTIONS_FILES.keys())

    def transform(self) -> bool:
        """Execute all Transparency transformations."""
        logger.info("🚀 Starting Transparency Portal Silver layer transformation...")
        
        success = True
        
        # Transform federal transfers
        if not self._transform_federal_transfers():
            success = False
        
        # Transform compliance sanctions
        if not self._transform_sanctions():
            success = False
        
        if success:
            logger.info("✅ Transparency Silver layer transformation complete!")
        else:
            logger.warning("⚠️ Transparency Silver layer transformation completed with errors")
        
        return success

    def _load_municipality_lookup(self) -> Dict[str, str]:
        """
        Load municipality lookup table from silver layer.
        
        Returns a dict mapping (normalized_name, state_abbrev) -> municipality_code
        """
        lookup_key = 'silver/dim_municipality_lookup/data.parquet'
        
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=lookup_key)
            df = pd.read_parquet(io.BytesIO(response['Body'].read()))
            
            # Build lookup dict: (normalized_name, state_abbrev) -> municipality_code
            lookup = {}
            for _, row in df.iterrows():
                key = (row['municipality_name_normalized'], row['state_abbrev'])
                lookup[key] = row['municipality_code']
            
            logger.info(f"📚 Loaded municipality lookup table: {len(lookup)} entries")
            return lookup
        except ClientError as e:
            logger.warning(f"⚠️ Could not load municipality lookup table: {e}")
            return {}

    def _normalize_municipality_name(self, name: str) -> str:
        """
        Normalize municipality name for lookup matching.
        
        Transformations:
        - Remove accents (é → e, ã → a, etc.)
        - Convert to uppercase
        - Remove apostrophes
        - Normalize whitespace
        
        :param name: Original municipality name.
        :return: Normalized name for matching.
        """
        if not name:
            return ""
        
        # Remove corrupted unicode replacement characters (encoding issues in bronze data)
        name = name.replace('�', '')
        
        # Remove accents using unicode normalization
        name = unicodedata.normalize('NFD', name)
        name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
        
        # Remove apostrophes
        name = name.replace("'", "")
        
        # Convert to uppercase
        name = name.upper()
        
        # Normalize whitespace
        name = ' '.join(name.split())
        
        return name

    def _lookup_municipality_code(self, municipality_name: str, state_abbrev: str, 
                                   lookup: Dict[str, str]) -> Optional[str]:
        """
        Look up municipality IBGE code from name and state.
        
        :param municipality_name: Municipality name from source data.
        :param state_abbrev: 2-letter state abbreviation.
        :param lookup: Municipality lookup dictionary.
        :return: 7-digit IBGE municipality code or None.
        """
        if not municipality_name or not state_abbrev or not lookup:
            return None
        
        normalized_name = self._normalize_municipality_name(municipality_name)
        state = state_abbrev.upper().strip()
        
        # Skip invalid entries
        if normalized_name in ('', 'SEM INFORMACAO') or state in ('', '-1'):
            return None
        
        key = (normalized_name, state)
        return lookup.get(key)

    def _extract_municipality_from_cnpj(self, cnpj: str) -> Optional[str]:
        """
        Try to extract municipality information from entity data.
        Note: CNPJ doesn't directly contain municipality codes.
        This is a placeholder for future enhancement with address data.
        """
        return None

    def _extract_municipality_from_agency_name(self, agency_name: str, state_abbrev: str) -> Optional[str]:
        """
        Extract municipality name from agency name patterns.
        
        Patterns matched:
        - "Prefeitura Municipal de Atibaia (SP)" -> "Atibaia"
        - "PREFEITURA DE UBIRATA - PR" -> "Ubirata"
        - "Tribunal de Justiça... / CANGUARETAMA / ..." -> "Canguaretama"
        - "1º Grau - TRF... / BOTUCATU / ..." -> "Botucatu"
        
        :param agency_name: Full agency name string.
        :param state_abbrev: 2-letter state abbreviation for validation.
        :return: Extracted municipality name or None.
        """
        if not agency_name:
            return None
        
        import re
        
        # Clean corrupted unicode before pattern matching
        # Remove replacement character and any non-ASCII characters that are corrupted
        agency_name_clean = ''.join(c for c in agency_name if ord(c) < 65533 or ord(c) > 65535)
        agency_name_clean = agency_name_clean.replace('\ufffd', '')
        
        # Pattern 1: "Prefeitura Municipal de X (UF)" or "Prefeitura Municipal de X - UF"
        pattern1 = r'Prefeitura\s+Municipal\s+de\s+(.+?)(?:\s*[\(\-]\s*[A-Z]{2}\s*[\)]?)?$'
        match = re.search(pattern1, agency_name_clean, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        
        # Pattern 2: "PREFEITURA DE X - UF" (alternate format)
        pattern2 = r'PREFEITURA\s+DE\s+([A-Z\s]+?)(?:\s*[\-]\s*[A-Z]{2})?$'
        match = re.search(pattern2, agency_name_clean)
        if match:
            return match.group(1).strip().title()
        
        # Pattern 3: "Câmara Municipal de X"
        pattern3 = r'C[âa]mara\s+Municipal\s+de\s+(.+?)(?:\s*[\(\-]\s*[A-Z]{2}\s*[\)]?)?$'
        match = re.search(pattern3, agency_name_clean, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        
        # Pattern 4: Court patterns "... / CITY_NAME / ..." (extract city from court hierarchy)
        # e.g., "Tribunal de Justiça... / CANGUARETAMA / 2ª VARA..."
        # Also handles "/ CUIABA / CUIABÁ -" pattern
        pattern4 = r'/\s*([A-ZÀ-Úa-zà-ú][A-ZÀ-Úa-zà-ú\s]+?)\s*/\s*(?:\d|VARA|JUIZADO|SEGUNDA|PRIMEIRA|TERCEIRA|[A-ZÀ-Ú]+\s*[\-\–])'
        match = re.search(pattern4, agency_name_clean)
        if match:
            city = match.group(1).strip()
            # Skip generic terms
            if city.upper() not in ('CAPITAL', 'SEDE', 'INTERIOR', 'VARA', 'SEÇÃO', 'CAPITAL SJPA'):
                return city.title()
        
        # Pattern 5: Federal court "... / Subseção Judiciária de X / ..." 
        pattern5 = r'Subse[çc][ãa]o\s+Judici[áa]ria\s+de\s+([A-Za-zÀ-ú\-\s]+?)(?:\s*/|$)'
        match = re.search(pattern5, agency_name_clean, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        
        # Pattern 6: "... VARA FEDERAL DE X" or "... VARA DE X"
        pattern6 = r'VARA\s+(?:FEDERAL\s+)?DE\s+([A-ZÀ-Ú][A-ZÀ-Ú\s]+?)(?:\s*$|\s*/)'
        match = re.search(pattern6, agency_name_clean)
        if match:
            city = match.group(1).strip()
            if city.upper() not in ('JURISDIÇÃO', 'FAZENDA', 'FAMÍLIA'):
                return city.title()
        
        # Pattern 7: "Comarca de X" pattern
        pattern7 = r'Comarca\s+de\s+([A-Za-zÀ-ú\s\-]+?)(?:\s*$|\s*/)'
        match = re.search(pattern7, agency_name_clean, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        
        # Pattern 8: "Capital SJXX / Nº City" - extract city after capital designation
        pattern8 = r'Capital\s+[A-Z]+\s*/\s*\d+[ºª]?\s*([A-Za-zÀ-ú]+)'
        match = re.search(pattern8, agency_name_clean)
        if match:
            return match.group(1).strip()
        
        # Pattern 9: "SUBSEÇÃO JUDICIÁRIA DE X" (handles corrupted unicode)
        # Match SUBSE + any chars + O + space + JUDICI + any chars + RIA
        pattern9 = r'SUBSE\w*O\s+JUDICI\w*RIA\s+DE\s+([A-Z][A-Z\s]+?)(?:\s*/|$)'
        match = re.search(pattern9, agency_name_clean)
        if match:
            city = match.group(1).strip()
            # Skip state names
            if city.upper() not in ('SANTA CATARINA', 'SAO PAULO', 'RIO DE JANEIRO', 'MINAS GERAIS'):
                return city.title()
        
        return None

    def _mask_document(self, document: str, doc_type: str = 'CNPJ') -> str:
        """
        Mask sensitive document numbers for privacy.
        
        :param document: CPF or CNPJ number.
        :param doc_type: Type of document ('CPF' or 'CNPJ').
        :return: Masked document string.
        """
        if not document:
            return None
        
        # Remove non-numeric characters
        clean_doc = re.sub(r'[^\d]', '', str(document))
        
        if doc_type == 'CPF' and len(clean_doc) == 11:
            # CPF: ***.***.XXX-XX (show last 5 digits)
            return f"***.***{clean_doc[6:9]}-{clean_doc[9:]}"
        elif doc_type == 'CNPJ' and len(clean_doc) == 14:
            # CNPJ: **.***.***/ XXXX-XX (show last 6 digits)
            return f"**.***.***/{ clean_doc[8:12]}-{clean_doc[12:]}"
        else:
            # Unknown format, mask most of it
            if len(clean_doc) > 4:
                return '*' * (len(clean_doc) - 4) + clean_doc[-4:]
            return clean_doc

    def _determine_entity_type(self, document: str) -> str:
        """
        Determine if entity is individual (PF) or company (PJ) based on document.
        
        :param document: CPF or CNPJ number.
        :return: 'PF' for individual, 'PJ' for company, 'UNKNOWN' otherwise.
        """
        if not document:
            return 'UNKNOWN'
        
        clean_doc = re.sub(r'[^\d]', '', str(document))
        
        if len(clean_doc) == 11:
            return 'PF'  # Pessoa Física (CPF)
        elif len(clean_doc) == 14:
            return 'PJ'  # Pessoa Jurídica (CNPJ)
        else:
            return 'UNKNOWN'

    def _list_federal_transfer_files(self) -> List[str]:
        """
        Discover all federal transfer monthly files in S3.
        
        Pattern: bronze/transparency/federal_transfers_YYYY_MM.json
        Returns: List of S3 keys
        """
        try:
            response = self.s3.list_objects_v2(
                Bucket=self.bucket,
                Prefix='bronze/transparency/federal_transfers_'
            )
            
            if 'Contents' not in response:
                return []
            
            # Filter for monthly pattern files only (exclude metadata)
            files = []
            for obj in response['Contents']:
                key = obj['Key']
                # Match pattern: federal_transfers_YYYY_MM.json
                if key.endswith('.json') and not key.endswith('.meta.json'):
                    # Verify it matches monthly pattern
                    import re
                    if re.search(r'federal_transfers_\d{4}_\d{2}\.json$', key):
                        files.append(key)
            
            logger.info(f"📁 Discovered {len(files)} federal transfer monthly files")
            return sorted(files)
        except Exception as e:
            logger.error(f"❌ Failed to list federal transfer files: {e}")
            return []
    
    def _transform_federal_transfers(self) -> bool:
        """
        Transform federal transfers data from monthly files across the intercensal window (2010-2022).
        
        Features:
        - Discovers monthly files dynamically from S3
        - Skips missing files gracefully
        - Idempotent: checks if output exists
        
        Output: silver/fact_federal_transfers/data.parquet
        """
        logger.info("📊 Transforming federal transfers data...")
        
        output_key = 'silver/fact_federal_transfers/data.parquet'
        metadata_key = 'silver/fact_federal_transfers/_metadata.json'
        
        # Discover all monthly files
        monthly_files = self._list_federal_transfer_files()
        
        if not monthly_files:
            logger.warning("⚠️ No federal transfer files found in bronze layer")
            return False
        
        # Smart caching: check if we should skip processing
        should_skip, skip_reason = self._should_skip_processing(output_key, metadata_key, monthly_files)
        if should_skip:
            logger.info(f"⏭️ Skipping federal_transfers: {skip_reason}")
            return True
        
        # Load municipality lookup table for cross-referencing
        municipality_lookup = self._load_municipality_lookup()
        
        all_records = []
        total_input = 0
        source_keys = []
        processed_count = 0
        skipped_count = 0
        matched_municipalities = 0
        
        for bronze_key in monthly_files:
            # Extract year and month from filename
            # Pattern: federal_transfers_YYYY_MM.json
            import re
            match = re.search(r'federal_transfers_(\d{4})_(\d{2})\.json$', bronze_key)
            if not match:
                logger.warning(f"⚠️ Skipping file with unexpected pattern: {bronze_key}")
                skipped_count += 1
                continue
            
            year = int(match.group(1))
            month = int(match.group(2))
            
            source_keys.append(bronze_key)
            data = self._read_bronze_json(bronze_key)
            
            if not data:
                logger.warning(f"⚠️ No data in {bronze_key} - skipping")
                skipped_count += 1
                continue
            
            total_input += len(data)
            processed_count += 1
            
            for record in data:
                # Parse the Transparency Portal format
                # Common fields vary by endpoint version
                
                # Try to extract relevant fields
                # Extract source agency - try multiple field patterns
                source_agency = (
                    record.get('nomeOrgaoSuperior') or  # Direct field from API
                    (record.get('orgaoSuperior', {}).get('nome') if isinstance(record.get('orgaoSuperior'), dict) else record.get('orgaoSuperior')) or
                    record.get('nomeOrgao') or
                    (record.get('unidadeGestora', {}).get('nome') if isinstance(record.get('unidadeGestora'), dict) else record.get('unidadeGestora')) or
                    record.get('nomeUG') or
                    'UNKNOWN'
                )
                
                transfer_record = {
                    'municipality_code': None,  # Will be populated below if available
                    'year': year,
                    'month': month,  # Use month from filename (more reliable)
                    'transfer_amount': self._safe_float(
                        record.get('valor') or 
                        record.get('valorRecebido') or 
                        record.get('valorTotal')
                    ),
                    'transfer_type': (
                        record.get('tipoTransferencia') or 
                        record.get('tipoPessoa') or
                        record.get('tipo') or 
                        record.get('descricao') or
                        'FEDERAL_TRANSFER'
                    ),
                    'source_agency': source_agency
                }
                
                # Try to extract municipality code from recipient info
                # Pattern 1: nested municipio object with codigoIBGE
                municipio = record.get('municipio', {})
                if isinstance(municipio, dict):
                    muni_code = municipio.get('codigoIBGE') or municipio.get('codigo')
                    if muni_code:
                        transfer_record['municipality_code'] = self._extract_municipality_code(muni_code)
                
                # Pattern 2: municipioPessoa (name) + siglaUFPessoa (state) - use lookup table
                if not transfer_record['municipality_code']:
                    muni_name = record.get('municipioPessoa')
                    state_abbrev = record.get('siglaUFPessoa')
                    if muni_name and state_abbrev:
                        muni_code = self._lookup_municipality_code(muni_name, state_abbrev, municipality_lookup)
                        if muni_code:
                            transfer_record['municipality_code'] = muni_code
                            matched_municipalities += 1
                
                # Skip records without valid amount
                if transfer_record['transfer_amount'] is None:
                    continue
                
                all_records.append(transfer_record)
        
        logger.info(f"📈 Processed {processed_count} files, skipped {skipped_count} files")
        logger.info(f"🏙️ Matched {matched_municipalities} records to municipality codes via lookup")
        
        if not all_records:
            logger.warning("⚠️ No federal transfer records extracted")
            self.log_processing('federal_transfers', 'FAILED', total_input, 0,
                              source_keys, output_key,
                              'No records extracted')
            return False
        
        df = pd.DataFrame(all_records)
        df = self.validate_schema(df, 'federal_transfers')
        
        # Remove duplicates based on key columns
        before_dedup = len(df)
        df = df.drop_duplicates(subset=['year', 'month', 'transfer_type', 'transfer_amount'], keep='first')
        after_dedup = len(df)
        if before_dedup > after_dedup:
            logger.info(f"🔄 Removed {before_dedup - after_dedup} duplicate records")
        
        # Sort by municipality, year, month
        df = df.sort_values(['municipality_code', 'year', 'month']).reset_index(drop=True)
        
        success = self._write_silver_parquet(df, output_key)
        self._write_silver_json(df, output_key.replace('.parquet', '.json'))
        
        # Save metadata tracking source files
        if success:
            source_file_hashes = {}
            for s3_key in source_keys:
                file_hash = self._get_object_digest(s3_key)
                if file_hash:
                    source_file_hashes[s3_key] = file_hash
            
            metadata = {
                'source_files': source_file_hashes,
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'record_count': len(df),
                'files_processed': processed_count
            }
            self._save_silver_metadata(metadata_key, metadata)
        
        self.log_processing('federal_transfers', 'SUCCESS' if success else 'FAILED',
                          total_input, len(df), source_keys[:10] + ['...'] if len(source_keys) > 10 else source_keys, output_key)
        
        logger.info(f"✅ Federal Transfers: {len(df)} records from {processed_count} monthly files")
        return success

    def _extract_month(self, record: Dict) -> Optional[int]:
        """Extract month from transfer record."""
        # Try various date fields
        date_fields = ['dataRecebimento', 'data', 'mesReferencia', 'mesAno']
        
        for field in date_fields:
            if field in record and record[field]:
                value = str(record[field])
                
                # Try to parse as date
                parsed = self._parse_date(value)
                if parsed:
                    return parsed.month
                
                # Try to extract month from MM/YYYY format
                match = re.match(r'(\d{1,2})/(\d{4})', value)
                if match:
                    return int(match.group(1))
        
        return None

    def _transform_sanctions(self) -> bool:
        """
        Transform compliance sanctions from CEIS, CNEP, CEAF, CEPIM.
        
        Output: silver/fact_sanctions/data.parquet
        """
        logger.info("📊 Transforming compliance sanctions data...")
        
        all_records = []
        total_input = 0
        source_keys = []
        matched_municipalities = 0
        
        output_key = 'silver/fact_sanctions/data.parquet'
        metadata_key = 'silver/fact_sanctions/_metadata.json'
        
        # Load municipality lookup for resolving municipality codes from agency names
        municipality_lookup = self._load_municipality_lookup()
        
        # Collect all potential source keys
        potential_sources = list(self.SANCTIONS_FILES.values())
        
        # Smart caching: check if we should skip processing
        should_skip, reason = self._should_skip_processing(output_key, metadata_key, potential_sources)
        if should_skip:
            logger.info(f"⏭️  Skipping sanctions: {reason}")
            return True
        
        registry_configs = {
            'ceis': {
                'key': 'ceis_sanctions',
                'type': 'CEIS',
                'entity_field': 'sancionado.nome',
                'doc_field': 'sancionado.codigoFormatado',
                'sanction_type_field': 'tipoSancao.descricaoResumida',
                'start_date_field': 'dataInicioSancao',
                'end_date_field': 'dataFimSancao',
                'agency_field': 'orgaoSancionador.nome',
                'uf_field': 'orgaoSancionador.siglaUf'
            },
            'cnep': {
                'key': 'cnep_sanctions',
                'type': 'CNEP',
                'entity_field': 'sancionado.nome',
                'doc_field': 'sancionado.codigoFormatado',
                'sanction_type_field': 'tipoSancao.descricaoResumida',
                'start_date_field': 'dataInicioSancao',
                'end_date_field': 'dataFimSancao',
                'agency_field': 'orgaoSancionador.nome',
                'uf_field': 'orgaoSancionador.siglaUf'
            },
            # CEAF excluded - location data unavailable at source (all records have "Sem informação")
            'cepim': {
                'key': 'cepim_sanctions',
                'type': 'CEPIM',
                'entity_field': 'sancionado.nome',
                'doc_field': 'sancionado.codigoFormatado',
                'sanction_type_field': 'tipoSancao.descricaoResumida',
                'start_date_field': 'dataInicioSancao',
                'end_date_field': 'dataFimSancao',
                'agency_field': 'orgaoSancionador.nome',
                'uf_field': 'orgaoSancionador.siglaUf'
            }
        }
        
        for registry_name, config in registry_configs.items():
            bronze_key = self.SANCTIONS_FILES.get(config['key'])
            if not bronze_key:
                logger.warning(f"⚠️ No bronze key configured for {config['key']}")
                continue
            
            source_keys.append(bronze_key)
            data = self._read_bronze_json(bronze_key)
            
            if not data:
                logger.warning(f"⚠️ No {config['type']} sanctions data - skipping")
                continue
            
            total_input += len(data)
            
            for idx, record in enumerate(data):
                # Extract entity name
                entity_name = self._extract_nested_value(record, config['entity_field'])
                if isinstance(entity_name, dict):
                    entity_name = entity_name.get('nome') or entity_name.get('razaoSocial') or str(entity_name)
                
                # Extract document (CPF/CNPJ)
                document = self._extract_nested_value(record, config['doc_field'])
                entity_type = self._determine_entity_type(document)
                
                # Extract sanction type
                sanction_type = self._extract_nested_value(record, config['sanction_type_field'])
                if isinstance(sanction_type, dict):
                    sanction_type = sanction_type.get('descricao') or sanction_type.get('nome') or str(sanction_type)
                
                # Extract dates
                start_date = self._parse_date(
                    self._extract_nested_value(record, config['start_date_field'])
                )
                
                end_date = None
                if config['end_date_field']:
                    end_date = self._parse_date(
                        self._extract_nested_value(record, config['end_date_field'])
                    )
                
                # Extract sanctioning agency
                agency = self._extract_nested_value(record, config['agency_field'])
                if isinstance(agency, dict):
                    agency = agency.get('nome') or agency.get('sigla') or str(agency)
                
                # Extract location info if available
                state_code = None
                
                # Try to get state from config uf_field first
                if config.get('uf_field'):
                    uf = self._extract_nested_value(record, config['uf_field'])
                    if uf:
                        state_code = self._uf_to_state_code(uf)
                
                # Fallback: try common UF fields
                if not state_code:
                    uf = record.get('ufSancionado') or record.get('uf')
                    if uf:
                        state_code = self._uf_to_state_code(uf)
                
                # If no state code yet, try to extract from agency name
                if not state_code and agency:
                    state_code = self._extract_state_from_agency(str(agency))
                
                # Try to extract municipality code from agency name (for municipal-level sanctions)
                municipality_code = None
                if agency and state_code:
                    # Get state abbreviation from state_code
                    state_abbrev = self._extract_nested_value(record, config.get('uf_field', ''))
                    if not state_abbrev:
                        state_abbrev = record.get('ufSancionado') or record.get('uf')
                    
                    if state_abbrev and state_abbrev not in ('Sem informação', 'Sem informacao', '-1'):
                        muni_name = self._extract_municipality_from_agency_name(str(agency), state_abbrev)
                        if muni_name:
                            muni_code = self._lookup_municipality_code(muni_name, state_abbrev, municipality_lookup)
                            if muni_code:
                                municipality_code = muni_code
                                matched_municipalities += 1
                
                # Generate unique sanction ID
                sanction_id = f"{config['type']}_{idx:08d}"
                if document:
                    doc_hash = hash(str(document)) % 10000000
                    sanction_id = f"{config['type']}_{doc_hash:07d}_{idx:05d}"
                
                sanction_record = {
                    'sanction_id': sanction_id,
                    'registry_type': config['type'],
                    'sanctioned_entity': str(entity_name)[:500] if entity_name else None,
                    'entity_type': entity_type,
                    'cpf_cnpj': self._mask_document(document, 'CPF' if entity_type == 'PF' else 'CNPJ'),
                    'sanction_type': str(sanction_type)[:200] if sanction_type else None,
                    'sanction_start_date': start_date,
                    'sanction_end_date': end_date,
                    'sanctioning_agency': str(agency)[:200] if agency else None,
                    'state_code': state_code,
                    'municipality_code': municipality_code
                }
                
                all_records.append(sanction_record)
        
        if not all_records:
            logger.warning("⚠️ No sanction records extracted")
            self.log_processing('compliance_sanctions', 'FAILED', total_input, 0,
                              source_keys, output_key,
                              'No records extracted')
            return False
        
        df = pd.DataFrame(all_records)
        df = self.validate_schema(df, 'compliance_sanctions')
        
        # Remove duplicates based on sanction_id
        before_dedup = len(df)
        df = df.drop_duplicates(subset=['sanction_id'], keep='first')
        after_dedup = len(df)
        if before_dedup > after_dedup:
            logger.info(f"🔄 Removed {before_dedup - after_dedup} duplicate records")
        
        # Sort by registry type and sanction ID
        df = df.sort_values(['registry_type', 'sanction_id']).reset_index(drop=True)
        success = self._write_silver_parquet(df, output_key)
        self._write_silver_json(df, output_key.replace('.parquet', '.json'))
        
        # Save metadata tracking source files
        if success:
            source_file_hashes = {}
            for s3_key in source_keys:
                file_hash = self._get_object_digest(s3_key)
                if file_hash:
                    source_file_hashes[s3_key] = file_hash
            
            metadata = {
                'source_files': source_file_hashes,
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'record_count': len(df)
            }
            self._save_silver_metadata(metadata_key, metadata)
        
        self.log_processing('compliance_sanctions', 'SUCCESS' if success else 'FAILED',
                          total_input, len(df), source_keys, output_key)
        
        # Log breakdown by registry
        registry_counts = df['registry_type'].value_counts().to_dict()
        logger.info(f"✅ Sanctions: {len(df)} total records")
        for registry, count in registry_counts.items():
            logger.info(f"   - {registry}: {count} records")
        
        return success

    def _extract_nested_value(self, record: Dict, field_path: str) -> Any:
        """
        Extract a value from a record, handling nested dictionaries.
        
        :param record: Source record dictionary.
        :param field_path: Field name or dot-separated path (e.g., 'orgao.nome').
        :return: Extracted value or None.
        """
        if not field_path:
            return None
        
        parts = field_path.split('.')
        value = record
        
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return None
        
        return value

    def _uf_to_state_code(self, uf: str) -> Optional[str]:
        """
        Convert UF abbreviation to state code.
        
        :param uf: UF abbreviation (e.g., 'SP', 'RJ') or state name.
        :return: 2-digit state code or None.
        """
        if not uf:
            return None
        
        uf = str(uf).strip().upper()
        
        # UF abbreviation to state code mapping
        uf_map = {
            'RO': '11', 'AC': '12', 'AM': '13', 'RR': '14', 'PA': '15', 'AP': '16', 'TO': '17',
            'MA': '21', 'PI': '22', 'CE': '23', 'RN': '24', 'PB': '25', 'PE': '26', 'AL': '27',
            'SE': '28', 'BA': '29', 'MG': '31', 'ES': '32', 'RJ': '33', 'SP': '35',
            'PR': '41', 'SC': '42', 'RS': '43', 'MS': '50', 'MT': '51', 'GO': '52', 'DF': '53'
        }
        
        # Direct lookup
        if uf in uf_map:
            return uf_map[uf]
        
        # If it's already a code, validate it
        if uf.isdigit() and uf in self.state_mapping:
            return uf
        
        return None

    def _extract_state_from_agency(self, agency: str) -> Optional[str]:
        """
        Extract state code from sanctioning agency name.
        
        Parses agency names to find state abbreviations or full state names.
        Examples:
        - "Governo do Estado da Bahia (BA)" -> '29'
        - "Prefeitura Municipal de Belo Horizonte - MG" -> '31'
        - "Seção Judiciária da Paraíba" -> '25'
        - "Tribunal de Justiça do Estado do Rio Grande do Norte" -> '24'
        
        :param agency: Sanctioning agency name.
        :return: 2-digit state code or None.
        """
        if not agency:
            return None
        
        agency_upper = str(agency).upper()
        
        # Pattern 1: UF in parentheses like "(BA)" or "(MG)"
        import re
        paren_match = re.search(r'\(([A-Z]{2})\)', agency_upper)
        if paren_match:
            uf = paren_match.group(1)
            state_code = self._uf_to_state_code(uf)
            if state_code:
                return state_code
        
        # Pattern 2: "- UF" at the end like "- MG" or "- SP"
        dash_match = re.search(r'[-–]\s*([A-Z]{2})\s*$', agency_upper)
        if dash_match:
            uf = dash_match.group(1)
            state_code = self._uf_to_state_code(uf)
            if state_code:
                return state_code
        
        # Pattern 3: Full state names
        state_names = {
            'RONDÔNIA': '11', 'RONDONIA': '11',
            'ACRE': '12',
            'AMAZONAS': '13',
            'RORAIMA': '14',
            'PARÁ': '15', 'PARA': '15',
            'AMAPÁ': '16', 'AMAPA': '16',
            'TOCANTINS': '17',
            'MARANHÃO': '21', 'MARANHAO': '21',
            'PIAUÍ': '22', 'PIAUI': '22',
            'CEARÁ': '23', 'CEARA': '23',
            'RIO GRANDE DO NORTE': '24',
            'PARAÍBA': '25', 'PARAIBA': '25',
            'PERNAMBUCO': '26',
            'ALAGOAS': '27',
            'SERGIPE': '28',
            'BAHIA': '29',
            'MINAS GERAIS': '31',
            'ESPÍRITO SANTO': '32', 'ESPIRITO SANTO': '32',
            'RIO DE JANEIRO': '33',
            'SÃO PAULO': '35', 'SAO PAULO': '35',
            'PARANÁ': '41', 'PARANA': '41',
            'SANTA CATARINA': '42',
            'RIO GRANDE DO SUL': '43',
            'MATO GROSSO DO SUL': '50',
            'MATO GROSSO': '51',
            'GOIÁS': '52', 'GOIAS': '52',
            'DISTRITO FEDERAL': '53'
        }
        
        for state_name, code in state_names.items():
            if state_name in agency_upper:
                return code
        
        # Pattern 4: City names that indicate states (for major cities)
        city_to_state = {
            'NATAL': '24',  # RN
            'JOÃO PESSOA': '25', 'JOAO PESSOA': '25',  # PB
            'RECIFE': '26',  # PE
            'MACEIÓ': '27', 'MACEIO': '27',  # AL
            'ARACAJU': '28',  # SE
            'SALVADOR': '29',  # BA
            'BELO HORIZONTE': '31',  # MG
            'VITÓRIA': '32', 'VITORIA': '32',  # ES
            'CURITIBA': '41',  # PR
            'FLORIANÓPOLIS': '42', 'FLORIANOPOLIS': '42',  # SC
            'PORTO ALEGRE': '43',  # RS
            'CAMPO GRANDE': '50',  # MS
            'CUIABÁ': '51', 'CUIABA': '51',  # MT
            'GOIÂNIA': '52', 'GOIANIA': '52',  # GO
            'BRASÍLIA': '53', 'BRASILIA': '53'  # DF
        }
        
        for city, code in city_to_state.items():
            if city in agency_upper:
                return code
        
        return None


if __name__ == "__main__":
    BUCKET_NAME = "enok-mba-thesis-datalake"
    CONFIG_FILE = Path(__file__).parent.parent.parent / "config" / "silver_schemas.json"
    AWS_PROFILE = "mba-thesis"
    
    transformer = TransparencyTransformer(BUCKET_NAME, str(CONFIG_FILE), aws_profile=AWS_PROFILE)
    transformer.transform()
