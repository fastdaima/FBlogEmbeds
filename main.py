import httpx
import tiktoken
from sqlite_utils.utils import  rows_from_file, Format
from sqlite_utils.utils import sqlite3
import sqlite_utils

sqlite3.enable_callback_tracebacks(True)

# embeddings model - will come to it later
def scrape_blog_data():
    pass

def extract_rows():
    pass

def create_embeddings(model_name, data):
    pass

def batch_rows(rows, batch_size):
    batch = []
    for row in rows:
        batch.append(row)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch: yield batch


def truncate_tokens(text, embed_size):
    encoding = tiktoken.get_encoding('cl100k_base')
    tokens = encoding.encode(text)
    tokens = tokens[:embed_size]
    return encoding.decode(tokens)



def embeddings(db_path, input_path, table_name='embeddings', sql=None, batch_size=64, embed_size=8100):
    """

    :param db_path:
    :param input_path:
    :param table_name:
    :param sql:
    :param batch_size:
    :param embed_size: maximum size can be 8192 , I think this is for sqlite db size
    :return:
    """
    db = sqlite_utils.Database(db_path)
    table = db[table_name]
    if not table.exists():
        table.create(
            {"id": str, "embeddings": bytes},
            pk="id"
        )
    if sql:
        rows = db.query(sql)
        print(len(rows))
        count_sql = 'select count(*) as c from ({})'.format(sql)
        expected_length = next(db.query(count_sql)['c'])
    else:
        raise ValueError("Only sqlite db is supported")

    total_tokens = 0
    skipped = 0

    for batch in batch_rows(rows, batch_size):
        text_to_embed = []
        ids_in_batch = []
        
        for row in batch: 
            values = list(row.values())
            id = values[0]
            try: 
                table.get(id)
                skipped += 1
                continue 
            except sqlite_utils.db.NotFoundError: 
                pass 
            text = ' '.join(v or '' for v in values[1:])
            ids_in_batch.append(id)
            text_to_embed.append(truncate_tokens(text, embed_size))
       
        if text_to_embed:
            # init local/ cloud embeddings object
            # create  embeddings
            pass



embeddings(
    'data/tils.db',
    '...',
    sql='select path, title from til'
)
