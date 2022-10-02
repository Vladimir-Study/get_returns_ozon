from datetime import datetime
from pprint import pprint
import asyncio
import os
import asyncpg
import aiohttp
import json
from dotenv import load_dotenv 
load_dotenv()


async def parse_fbs_return(client_id: str, api_key: str, limit: int = 1000, offset: int = 0):
    url = 'https://api-seller.ozon.ru/v2/returns/company/fbs'
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
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
                                            headers=headers
                                            ) as response:
                        resp_orders = await response.json()
                        if response.status == 200:
                            if resp_orders is None:
                                sleep(3)
                                end_while += 1
                                print(f'Repeat request: {end_while}')
                            return_orders = [*return_orders, *resp_orders['result']['returns']]
                            print(f"Number of ofders for parsing: {len(return_orders)}")
                            len_order = len(resp_orders['result']['returns'])
                            count_offset += 1
                            offset = count_offset*1000
                        else:
                            sleep(3)
                            end_while += 1
                            print(f'Repeat request: {end_while}')
                except aiohttp.client_exceptions as E:
                    print(f'Error get data returns FBS: {E}')
            return return_orders
        except Exception as E:
            print(f'Exception get return orders ozon FBS : {E}')


async def get_chunk_fbs(accounts_list: dict, status_dict: dict):
    try:
        async with asyncpg.create_pool(
                host='rc1b-itt1uqz8cxhs0c3d.mdb.yandexcloud.net',
                port='6432',
                database='market_db',
                user=os.environ['DB_LOGIN'],
                password=os.environ['DB_PASSWORD'],
                ssl='require'
                ) as pool:
            for key, account in accounts_list.items():
                returns = await parse_fbs_return(
                    account['client_id_api'],
                    account['api_key']
                )
                tasks = []
                count = 0
                chunk = 1000
                for order in returns:
                    count += 1
                    task = asyncio.create_task(fbs_send_to_return_table(order, status_dict, pool))
                    tasks.append(task)
                    if len(tasks) == chunk or count == len(returns):
                        await asyncio.gather(*tasks[:chunk])
                        tasks = []
                        print(f'Send to DB: {count}')
            await delete_dublicate(pool)
    except Exception as E:
        print(f'Errors in main return FBS: {E}')


async def make_request_insert_in_db(pool, order):
    insert = await pool.execute('''INSERT INTO return_table VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 
                                $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26)''', str(order['id']),
                                str(order['clearing_id']), order['posting_number'], str(order['product_id']),
                                str(order['sku']), order['status'], order['returns_keeping_cost'],
                                order['return_reason_name'], 
                                None if order['return_date'] == None else datetime.strptime(
                                    order['return_date'][:19], '%Y-%m-%dT%H:%M:%S'),
                                order['quantity'],order['product_name'], order['price'], 
                                None if order['waiting_for_seller_date_time'] == None else datetime.strptime(
                                    order['waiting_for_seller_date_time'][:19], '%Y-%m-%dT%H:%M:%S'),
                                None if order['returned_to_seller_date_time'] == None else datetime.strptime(
                                    order['returned_to_seller_date_time'][:19], '%Y-%m-%dT%H:%M:%S'),
                                None if order['last_free_waiting_day'] == None else datetime.strptime(
                                    order['last_free_waiting_day'][:19], '%Y-%m-%dT%H:%M:%S'),
                                str(order['is_opened']), str(order['place_id']), order['commission_percent'],
                                order['commission'], order['price_without_commission'], str(order['is_moving']),
                                order['moving_to_place_name'], order['waiting_for_seller_days'],
                                str(order['picking_amount']), str(order['accepted_from_customer_moment']),
                                str(order['picking_tag']))
    return insert


async def make_request_update_in_db(pool, order):
    update = await pool.execute('''UPDATE SET return_table status = $1''', order['status'])
    return update
        

async def fbs_send_to_return_table(order, status_in_db, pool):
    try:
        if order['id'] in status_in_db.keys() and \
                status_in_db['status'] != order['status_name']:
            task = asyncio.create_task(make_request_update_in_db(pool, order))
        elif order['id'] not in status_in_db.keys():
            task = asyncio.create_task(make_request_insert_in_db(pool, order))
        return task
    except Exception as E:
        print(E)


if __name__ == '__main__':
    pass
