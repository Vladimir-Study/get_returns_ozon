from dotenv import load_dotenv
from pprint import pprint
from time import sleep
import os
import asyncio
import asyncpg
import aiohttp
from datetime import datetime

load_dotenv()


async def parse_fbo_return(
                            client_id: str,
                            api_key: str,
                            limit: int = 1000,
                            offset: int = 0
                            ):
    url = 'https://api-seller.ozon.ru/v2/returns/company/fbo'
    ozon_headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                      'AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
        'Content-Type': 'application/json',
        'Client-Id': client_id,
        'Api-Key': api_key,
    }
    async with aiohttp.ClientSession() as session:
        return_orders = []
        len_order = 1000
        end_while = 0
        count_offset = 0
        try:
            print(api_key)
            while len_order == 1000 and end_while != 10:
                try:
                    data = {
                        "limit": limit,
                        "offset": offset,
                    }
                    async with session.post(url,
                                            json=data,
                                            headers=ozon_headers
                                            ) as response:
                        resp_orders = await response.json()
                        if response.status == 200:
                            return_orders = [*return_orders, *resp_orders['returns']]
                            print(f"Number of ofders for parsing: {len(return_orders)}")
                            len_order = len(resp_orders['returns'])
                            count_offset += 1
                            offset = count_offset*1000
                        else:
                            sleep(3)
                            end_while += 1
                            print(f'Repeat request: {end_while}')
                except aiohttp.client_exceptions as E:
                    print(f'Error get data returns FBO: {E}')
            return return_orders
        except Exception as E:
            print(f'Exception get return orders ozon FBO : {E}')


async def make_request_insert_in_db_fbo(pool, row):
    insert = await pool.execute('''INSERT INTO return_table (return_id, 
    posting_number, sku, status, return_reason_name, return_date, 
    waiting_for_seller_date_time, is_opened, moving_to_place_name) VALUES 
    ($1, $2, $3, $4, $5, $6, $7, $8, $9)''', str(row['id']), row['posting_number'],
                                str(row['sku']), row['status_name'], row['return_reason_name'],
                                None if row['accepted_from_customer_moment'] == '' or 'null' else datetime.strptime(
                                    row['accepted_from_customer_moment'][:19], '%Y-%m-%dT%H:%M:%S'),
                                None if row['returned_to_ozon_moment'] == '' or 'null' else datetime.strptime(
                                    row['returned_to_ozon_moment'][:19], '%Y-%m-%dT%H:%M:%S'),
                                str(row['is_opened']), row['dst_place_name']
                                )
    return insert


async def make_request_update_in_db_fbo(pool, row):
    update = await pool.execute('''UPDATE return_table SET status = $1 
            WHERE return_id = $2''', row['status_name'], str(row['id']))
    return update


async def fbo_send_to_return_table(return_order: dict, status_in_db, pool) -> None:
    try:
        if return_order['id'] not in status_in_db.keys():
            task = asyncio.create_task(make_request_insert_in_db_fbo(pool, return_order))
        elif return_order['id'] in status_in_db.keys() and \
                status_in_db['status'] != return_order['status_name']:
            task = asyncio.create_task(make_request_update_in_db_fbo(pool, return_order))
        return task
    except (Exception) as E:
        print(f"Error in send to return table fbo: {E}")


async def get_chunk_fbo(accounts_list: dict, status_dict: dict) -> None:
    try:
        async with asyncpg.create_pool(
                host='rc1b-itt1uqz8cxhs0c3d.mdb.yandexcloud.net',
                port='6432',
                database='market_db',
                user=os.environ['DB_LOGIN'],
                password=os.environ['DB_PASSWORD'],
                ssl='require'
                ) as pool:
            try:
                for key, account in accounts_list.items():
                    returns = await parse_fbo_return(
                        account['client_id_api'],
                        account['api_key']
                    )
                    tasks = []
                    count = 0
                    chunk = 1000
                    for order in returns:
                        count += 1
                        task = asyncio.create_task(fbo_send_to_return_table(order, status_dict, pool))
                        tasks.append(task)
                        if len(tasks) == chunk or count == len(returns):
                            await asyncio.gather(*tasks[:chunk])
                            tasks = []
                            print(f'Send to DB: {count}')
            except asyncpg.exceptions as E:
                print(f'Errors in set to BD: {E}')
            await delete_dublicate(pool)
    except Exception as E:
        print(f'Errors in main return FBO: {E}')


if __name__ == '__main__':
    pass
