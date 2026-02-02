"""
Transparency Portal Data Transformer for Silver Layer

Transforms Transparency Portal data from Bronze to Silver layer:
- Federal Transfers (2013-2017, monthly files)
- Compliance Sanctions (CEIS, CNEP, CEAF, CEPIM)

Features:
- Idempotent: Safe to run multiple times
- Graceful: Skips missing files without failing
- Dynamic: Discovers monthly files automatically
"""

import logging
import re
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
        'ceaf_sanctions': 'bronze/transparency/ceaf_compliance.json',
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

    def _extract_municipality_from_cnpj(self, cnpj: str) -> Optional[str]:
        """
        Try to extract municipality information from entity data.
        Note: CNPJ doesn't directly contain municipality codes.
        This is a placeholder for future enhancement with address data.
        """
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
        Transform federal transfers data from monthly files (2013-2017).
        
        Features:
        - Discovers monthly files dynamically from S3
        - Skips missing files gracefully
        - Idempotent: checks if output exists
        
        Output: silver/fact_federal_transfers/data.parquet
        """
        logger.info("📊 Transforming federal transfers data...")
        
        output_key = 'silver/fact_federal_transfers/data.parquet'
        metadata_key = 'silver/fact_federal_transfers/.metadata.json'
        
        # Discover all monthly files
        monthly_files = self._list_federal_transfer_files()
        
        if not monthly_files:
            logger.warning("⚠️ No federal transfer files found in bronze layer")
            return False
        
        # Smart caching: check if we should skip processing
        if self._should_skip_processing(output_key, metadata_key, monthly_files):
            return True
        
        all_records = []
        total_input = 0
        source_keys = []
        processed_count = 0
        skipped_count = 0
        
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
                transfer_record = {
                    'municipality_code': None,  # Not directly available in this endpoint
                    'year': year,
                    'month': month,  # Use month from filename (more reliable)
                    'transfer_amount': self._safe_float(
                        record.get('valor') or 
                        record.get('valorRecebido') or 
                        record.get('valorTotal')
                    ),
                    'transfer_type': (
                        record.get('tipoTransferencia') or 
                        record.get('tipo') or 
                        record.get('descricao') or
                        'FEDERAL_TRANSFER'
                    ),
                    'source_agency': (
                        record.get('orgaoSuperior', {}).get('nome') if isinstance(record.get('orgaoSuperior'), dict) else
                        record.get('orgaoSuperior') or
                        record.get('unidadeGestora', {}).get('nome') if isinstance(record.get('unidadeGestora'), dict) else
                        record.get('unidadeGestora') or
                        'UNKNOWN'
                    )
                }
                
                # Try to extract municipality code from recipient info
                municipio = record.get('municipio', {})
                if isinstance(municipio, dict):
                    muni_code = municipio.get('codigoIBGE') or municipio.get('codigo')
                    if muni_code:
                        transfer_record['municipality_code'] = self._extract_municipality_code(muni_code)
                
                # Skip records without valid amount
                if transfer_record['transfer_amount'] is None:
                    continue
                
                all_records.append(transfer_record)
        
        logger.info(f"📈 Processed {processed_count} files, skipped {skipped_count} files")
        
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
                file_hash = self._get_bronze_file_hash(s3_key)
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
        
        output_key = 'silver/fact_sanctions/data.parquet'
        metadata_key = 'silver/fact_sanctions/.metadata.json'
        
        # Collect all potential source keys
        potential_sources = list(self.SANCTIONS_FILES.values())
        
        # Smart caching: check if we should skip processing
        if self._should_skip_processing(output_key, metadata_key, potential_sources):
            return True
        
        registry_configs = {
            'ceis': {
                'key': 'ceis_sanctions',
                'type': 'CEIS',
                'entity_field': 'sancionado',
                'doc_field': 'cpfCnpjSancionado',
                'sanction_type_field': 'tipoSancao',
                'start_date_field': 'dataInicioSancao',
                'end_date_field': 'dataFimSancao',
                'agency_field': 'orgaoSancionador'
            },
            'cnep': {
                'key': 'cnep_sanctions',
                'type': 'CNEP',
                'entity_field': 'sancionado',
                'doc_field': 'cpfCnpjSancionado',
                'sanction_type_field': 'tipoSancao',
                'start_date_field': 'dataInicioSancao',
                'end_date_field': 'dataFimSancao',
                'agency_field': 'orgaoSancionador'
            },
            'ceaf': {
                'key': 'ceaf_sanctions',
                'type': 'CEAF',
                'entity_field': 'nome',
                'doc_field': 'cpf',
                'sanction_type_field': 'tipoPunicao',
                'start_date_field': 'dataPublicacao',
                'end_date_field': 'dataFimPunicao',
                'agency_field': 'orgao'
            },
            'cepim': {
                'key': 'cepim_sanctions',
                'type': 'CEPIM',
                'entity_field': 'convenente',
                'doc_field': 'cnpjEntidade',
                'sanction_type_field': 'motivoImpedimento',
                'start_date_field': 'dataReferencia',
                'end_date_field': None,
                'agency_field': 'orgaoConcedente'
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
                municipality_code = None
                
                uf = record.get('ufSancionado') or record.get('uf')
                if uf:
                    # Convert UF name/abbrev to code if needed
                    state_code = self._uf_to_state_code(uf)
                
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
                file_hash = self._get_bronze_file_hash(s3_key)
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


if __name__ == "__main__":
    BUCKET_NAME = "enok-mba-thesis-datalake"
    CONFIG_FILE = Path(__file__).parent.parent.parent / "config" / "silver_schemas.json"
    
    transformer = TransparencyTransformer(BUCKET_NAME, str(CONFIG_FILE))
    transformer.transform()
