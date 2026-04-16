import sqlglot
from sqlglot import exp
import os
import logging
import re
from datetime import datetime

# ==========================================
# [환경 설정] 에러 보관함 및 로깅 세팅
# ==========================================
class BufferedSqlglotHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.messages = []

    def emit(self, record):
        self.messages.append(record.getMessage())

    def add_error(self, error_msg):
        self.messages.append(error_msg)

    def clear(self):
        self.messages.clear()

buffer_handler = BufferedSqlglotHandler()

sqlglot_logger = logging.getLogger("sqlglot")
sqlglot_logger.setLevel(logging.WARNING)
sqlglot_logger.propagate = False 
sqlglot_logger.addHandler(buffer_handler)


def auto_parse_sql(sql_query):
    dialects_to_try = ["oracle", "postgres", "mysql", "tsql", None]
    
    # =================================================================
    # [사전 정제] 완벽한 오라클 & MyBatis 대응
    # =================================================================
    # 1. CDATA 껍질 제거 (안의 부등호 <, > 등은 그대로 쿼리에 노출)
    query = sql_query.replace('<![CDATA[', ' ').replace(']]>', ' ')
    
    # 2. foreach 구문을 SQL 문법에 맞게 ('dummy') 로 치환
    query = re.sub(r'(?i)<foreach\b[^>]*>.*?</foreach>', "('dummy')", query, flags=re.DOTALL)
    
    # 3. MyBatis/XML 주석 제거
    query = re.sub(r'', ' ', query, flags=re.DOTALL)
    
    # 4. 기타 MyBatis 제어 태그 제거 (태그 껍질만 날리고 알맹이는 보존)
    mybatis_tags = r'</?(?:if|choose|when|otherwise|bind|sql|include|select|insert|update|delete)\b.*?>'
    query = re.sub(mybatis_tags, ' ', query, flags=re.IGNORECASE | re.DOTALL)
    
    # 5. 바인딩 변수 치환 #{...}, ${...} -> '1'
    query = re.sub(r'[#$]\{.*?\}', "'1'", query)
    
    # 6. 오라클 전용 파싱 방해꾼 제거
    query = re.sub(r'(?i)\bON\s+OVERFLOW\s+TRUNCATE\b', ' ', query) # LISTAGG 내부 방해꾼 제거
    query = re.sub(r'(?i)\bNOLOGGING\b', ' ', query) # CREATE TABLE 내부 방해꾼 제거
    
    # 7. 세미콜론 강제 주입
    pattern = r'(?im)^(\s*)(CREATE\s+TABLE|DROP\s+TABLE|TRUNCATE\s+TABLE|MERGE\s|INSERT\s+(?:/\*.*?\*/\s*)?INTO|DELETE\s+FROM)'
    query = re.sub(pattern, r';\1\2', query)
    
    # --- 파싱 시도 ---
    for dialect in dialects_to_try:
        try:
            parsed_list = sqlglot.parse(query, read=dialect)
            if parsed_list and all(p and not isinstance(p, exp.Command) for p in parsed_list):
                return parsed_list
        except Exception:
            pass
            
    valid_parsed = []
    queries = query.split(';')
    
    for q in queries:
        q_strip = q.strip()
        if not q_strip:
            continue
            
        q_no_comment = re.sub(r'/\*.*?\*/', '', q_strip, flags=re.DOTALL)
        q_no_comment = re.sub(r'--.*', '', q_no_comment).strip()
        
        if q_no_comment.upper().startswith(('EXEC', 'O_COUNT', 'COMMIT', 'ROLLBACK', 'DECLARE', 'BEGIN', '/', 'ALTER SESSION')):
            continue
            
        parsed_success = False
        last_error = None
        for dialect in dialects_to_try:
            try:
                parsed = sqlglot.parse_one(q_strip, read=dialect)
                if parsed and not isinstance(parsed, exp.Command):
                    valid_parsed.append(parsed)
                    parsed_success = True
                    break
            except Exception as e:
                last_error = str(e)
                
        if not parsed_success:
            err_msg = last_error.split('\n')[0] if last_error else "지원하지 않는 문법 구조"
            clean_query = " ".join(q_strip.split())
            query_preview = clean_query[:80] + "..." if len(clean_query) > 80 else clean_query
            buffer_handler.add_error(f"파싱 실패: {err_msg} \n          -> [쿼리 내용] {query_preview}")
                
    if valid_parsed:
        return valid_parsed
        
    return []

def fallback_extract_tables(sql_query):
    """
    [최후의 보루] sqlglot 파싱이 완전히 실패했을 때 정규식으로 테이블 강제 추출
    """
    clean_sql = re.sub(r'--.*', '', sql_query)
    clean_sql = re.sub(r'/\*.*?\*/', '', clean_sql, flags=re.DOTALL)
    
    # FROM, JOIN, INTO, UPDATE 뒤에 오는 [데이터베이스.스키마.테이블] 구조 강제 추출
    pattern = r'(?i)\b(?:FROM|JOIN|INTO|UPDATE)\s+([a-zA-Z0-9_#]+(?:\.[a-zA-Z0-9_#]+)*)\b'
    matches = re.findall(pattern, clean_sql)
    
    tables = set()
    for tb in matches:
        tb_lower = tb.lower()
        # 예약어가 아닌 진짜 테이블명만 걸러냄
        if tb_lower not in ('select', 'where', 'group', 'order', 'inner', 'left', 'right', 'outer', 'cross', 'from', 'join', 'on', 'using', 'as', 'lateral', 'natural'):
            tables.add(tb_lower)
    return tables

def get_full_table_name(table_node):
    parts = []
    for part_key in ["catalog", "db", "this"]:
        node = table_node.args.get(part_key)
        if node:
            name_str = node.name if hasattr(node, "name") else str(node)
            clean_name = name_str.replace('"', '').replace('`', '').replace('[', '').replace(']', '').strip()
            if clean_name:
                parts.append(clean_name.lower())
    return ".".join(parts)

def extract_tables_from_query(sql_query):
    parsed_list = auto_parse_sql(sql_query)
    actual_tables = set()
    
    # 1. 메인 파서(sqlglot) 추출
    if parsed_list:
        for parsed in parsed_list:
            cte_names = {cte.alias.lower() for cte in parsed.find_all(exp.CTE)}
            for table in parsed.find_all(exp.Table):
                if not table.args.get("this"):
                    continue
                full_name = get_full_table_name(table)
                table_only_name = full_name.split('.')[-1]
                if full_name and table_only_name not in cte_names:
                    actual_tables.add(full_name)
                    
    # 2. [강력한 안전망] 파서가 실패하여 테이블을 단 1개도 찾지 못했을 경우 -> 정규식 특공대 투입!
    if not actual_tables:
        logging.info("  -> 구문 분석에 실패하여 정규식(Regex) 안전망을 통해 테이블 추출을 시도합니다.")
        actual_tables = fallback_extract_tables(sql_query)
        
    if not actual_tables:
        return actual_tables

    # 3. 중복 제거
    sorted_raw = sorted(list(actual_tables), key=len, reverse=True)
    final_tables = set()
    
    for tb in sorted_raw:
        is_duplicate = False
        for existing in final_tables:
            if existing == tb or existing.endswith("." + tb):
                is_duplicate = True
                break
        if not is_duplicate:
            final_tables.add(tb)
            
    return final_tables

def process_all_sql_files(input_folder, output_filepath):
    if not os.path.exists(input_folder):
        logging.error(f"오류: '{input_folder}' 폴더를 찾을 수 없습니다. 폴더를 생성해주세요.")
        return

    target_files = []
    for filename in os.listdir(input_folder):
        if filename.lower().endswith(('.sql', '.txt')):
            target_files.append(os.path.join(input_folder, filename))
    
    if not target_files:
        logging.warning(f"'{input_folder}' 폴더 안에 .sql 또는 .txt 파일이 없습니다.")
        return

    all_extracted_tables = set()
    logging.info(f"총 {len(target_files)}개의 파일을 분석합니다...\n")

    for filepath in target_files:
        filename = os.path.basename(filepath)
        logging.info(f"[{filename}] 분석 중...")
        
        try:
            with open(filepath, 'r', encoding='utf-8') as file:
                sql_query = file.read()
        except UnicodeDecodeError:
            try:
                with open(filepath, 'r', encoding='cp949') as file:
                    sql_query = file.read()
            except Exception as e:
                logging.error(f"  -> 파일 읽기 실패 (인코딩 문제): {e}")
                continue
            
        if not sql_query.strip():
            logging.info("  -> 내용이 비어있어 스킵합니다.")
            continue
        
        buffer_handler.clear()
        
        tables_in_file = extract_tables_from_query(sql_query)
        
        if not tables_in_file:
            logging.warning("  -> 경고: 파일 내에서 유효한 테이블 쿼리를 추출하지 못했습니다.")
            unique_messages = list(dict.fromkeys(buffer_handler.messages))
            for msg in unique_messages[:3]:
                logging.warning(f"     [실패 원인] {msg}")
            
            if len(unique_messages) > 3:
                logging.warning(f"     ... 외 {len(unique_messages) - 3}건의 오류 존재")
        else:
            logging.info(f"  -> 분석 완료 (추출된 테이블: {len(tables_in_file)}개)")
            all_extracted_tables.update(tables_in_file)

    sorted_tables = sorted(list(all_extracted_tables))
    with open(output_filepath, 'w', encoding='utf-8') as f:
        for tb in sorted_tables:
            f.write(f"{tb}\n")
            
    logging.info(f"\n✅ 전체 분석 완료! 총 {len(sorted_tables)}개의 고유 테이블이 '{output_filepath}'에 저장되었습니다.")

# ==========================================
# [실행부]
# ==========================================
if __name__ == "__main__":
    input_folder = "input_sql"
    output_folder = "output_txt"
    log_folder = "log"

    os.makedirs(output_folder, exist_ok=True)
    os.makedirs(log_folder, exist_ok=True)
    output_txt_file = os.path.join(output_folder, "table_list.txt")
    
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filepath = os.path.join(log_folder, f"{current_time}_parser.log")

    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
        
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(message)s")
    
    file_handler = logging.FileHandler(log_filepath, encoding='utf-8')
    file_handler.setFormatter(formatter)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    logging.info("=== SQL 파싱 자동화 스크립트 시작 ===")
    process_all_sql_files(input_folder, output_txt_file)
    logging.info(f"\n=== 작업 완료 (상세 로그 확인: {log_filepath}) ===")