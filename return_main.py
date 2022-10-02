from pprint import pprint
from return_fbs import get_chunk_fbs
from return_fbo import get_chunk_fbo
from dotenv import load_dotenv
import os
import asyncio
import asyncpg

load_dotenv()


async def delete_dublicate(pool) -> None:
    async with pool.acquire() as conn:
        table_one = await conn.execute('DELETE FROM return_table WHERE ctid IN (SELECT ctid FROM (SELECT *, '
                           'ctid, row_number()OVER (PARTITION BY return_id ORDER BY id DESC) FROM return_table)s '
                           'WHERE row_number >= 2)'
                           )
        print(f'Deletion of duplicates in the table of returns is completed. Count: {table_one}')


async def get_status_order_in_db(pool) -> dict:
    async with pool.acquire() as conn:
        status = await conn.fetch('SELECT return_id, status FROM return_table')
        status_dict = {}
        for record in status:
            order_status = {}
            order_status[record['return_id']] = record['status']
            status_dict = {**status_dict, **order_status}
        return status_dict


async def account_data(pool) -> dict: 
    try:
        return_dict = {}
        async with pool.acquire() as conn:
            select_result_db = await conn.fetch('SELECT * FROM account_list WHERE mp_id = 1')
            for line in select_result_db:
                if line[9] == 'Active':
                    if line[6] in [values['api_key'] for values in return_dict.values()]:
                        continue
                    else:
                        return_dict[line[0]] = {'client_id_api': line[5], 'api_key': line[6]}
            return return_dict
    except (Exception, Error) as E:
        print(f'Error {E}')


async def main() -> None:
    async with asyncpg.create_pool(
            host='rc1b-itt1uqz8cxhs0c3d.mdb.yandexcloud.net',
            port='6432',
            database='market_db',
            user=os.environ['DB_LOGIN'],
            password=os.environ['DB_PASSWORD'],
            ssl='require' 
            ) as pool:
        account_list = await account_data(pool)
        status_dict = await get_status_order_in_db(pool)
        await get_chunk_fbo(account_list, status_dict)
        await get_chunk_fbs(account_list, status_dict)
        await delete_dublicate(pool)        

    
if __name__ == '__main__':
    asyncio.run(main())
