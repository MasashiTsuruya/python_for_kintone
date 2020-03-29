import requests
import pymysql
import datetime
import json
import sys
import os

host      = 'ホスト名'
user      = 'ユーザ名'
password  = 'パスワード'
db        = 'データベース名'
table     = 'テーブル名'
api_token = 'kintoneアプリのapiトークン'
app       = 'アプリ番号(文字列ではなくint型)'

maria     = []
kin_id    = []
maria_mod = {}
kin_mod   = {}

class MariaDB:
  def __init__(self, host, user, password, db, table):
    self.host     = host
    self.user     = user
    self.password = password
    self.db       = db
    self.table    = table

  def get_mariadb(self, field, where):
    """field      取得するカラム
       where      kintoneとDBの差分を取得するための条件
                  無い場合は空文字を入れる
    """
    conn = pymysql.connect(
      host        = self.host,
      user        = self.user,
      password    = self.password,
      db          = self.db,
      charset     = 'utf8',
      cursorclass = pymysql.cursors.DictCursor
    )
    try:
      with conn.cursor() as cursor:
        sql     = 'SELECT {0} FROM {1} WHERE modified = date(now()) {2} ORDER BY id desc limit 10'.format(field, self.table, where)
        cursor.execute(sql)
        result  = cursor.fetchall()
    finally:
      conn.close()

    return result


class Kintone:
  def __init__(self, api_token, app):
    self.api_token = api_token
    self.app       = app

  def get_kintone(self, field):
    """kintoneのレコードを取得する関数"""
    url     = "https://zenk.cybozu.com/k/v1/records.json?app={}&query=更新日時%20%3D%20TODAY()&fields={}&fields=id&fields=created_at".format(str(self.app), field)
    headers = {"X-Cybozu-API-Token": self.api_token}
    resp    = requests.get(url, headers=headers)

    return json.loads(resp.content.decode('utf-8'))

  def post_kintone(self, result):
    """kintoneのレコードを更新する関数"""
    url    = "https://zenk.cybozu.com/k/v1/records.json"
    params = {
      "app": self.app,
      "records": [

      ]
    }
    for idx in range(len(result)):
      params['records'].append({})
      for key, value in result[idx].items():
        if key == '案件名':
          dir_path = '/var/zenk/juchu/{}'.format(value)
          os.makedirs(dir_path)
          for i in range(6):
            dir_paths = dir_path + '/test{}'.format(i)
            os.makedirs(dir_paths)
        if value == '0000-00-00':
          continue
        if key == 'modified':
          continue
        if key == 'created_at':
          value = str(value)
          value = value[:-3]
        if key == '要求納期':
          key = '顧客要求納期'
          value = str(value)
        if key == '回答納期':
          value = str(value)
        if key == '案件ステータス備考':
          key = 'ステータス備考'

        params['records'][idx][key] = {
          'value': value
        }
    headers  = {"X-Cybozu-API-Token": self.api_token, "Content-Type" : "application/json"}
    resp     = requests.post(url, json=params, headers=headers)


    return resp

  def put_kintone(self, result):
    """kintoneのレコードを更新する関数"""
    url = "https://zenk.cybozu.com/k/v1/records.json"
    params = {
      "app": self.app,
      "records": [

      ]
    }
    rec = self.get_kintone('$id')
    for idx, record in enumerate(rec['records']):
      if '14' in record['id']['value']:
        print('true')

    for idx in range(len(result)):
      params['records'].append({})
      params['records'][idx]['id'] = result[idx]['id']
      params['records'][idx]['record'] = {}
      for key, value in result[idx].items():
        if value == '0000-00-00':
          continue
        if key == 'modified':
          continue
        if key == 'created_at':
          value = str(value)
          value = value[:-3]
        if key == '要求納期':
          key = '顧客要求納期'
          value = str(value)
        if key == '回答納期':
          value = str(value)
        if key == '案件ステータス備考':
          key = 'ステータス備考'

        params['records'][idx]['record'][key] = {
          'value': value
        }
    headers = {"X-Cybozu-API-Token": self.api_token, "Content-Type" : "application/json"}
    resp = requests.put(url, json=params, headers=headers)

    return resp

#クラスのインスタンス
mariaDB = MariaDB(host, user, password, db, table)
kintone = Kintone(api_token, app)

def check_record():
  #更新日が当日のテーブル一覧
  current_id = mariaDB.get_mariadb('id, created_at', '')

  #id, created_atカラムをセット
  for val in current_id:
    maria.append(str(val['id']))
    maria_mod[str(val['id'])] = str(val['created_at'])

  #更新日が当日のkintoneレコード取得
  records = kintone.get_kintone('$id')

  #idカラムをセット
  for val in records['records']:
    kin_id.append(val['id']['value'])
    kin_mod[val['id']['value']] = val['created_at']['value']

  #created_atに差があるかどうか
  diff_list       = maria_mod.items() - kin_mod.items()
  diff_list_keys  = dict(diff_list).keys()
  diff_list_tuple = tuple(diff_list_keys)

  #差がある場合は差分を取得する条件文を作成
  _diff_len = set(maria) - set(kin_id)
  diff_len  = tuple(_diff_len)

  if len(diff_len) == 0:
    where = 'AND id in {}'.format(diff_list_tuple)
    if len(diff_list_tuple) == 1:
      where  = where[:-2] + ')'
    result   = mariaDB.get_mariadb('*', where)
    response = kintone.put_kintone(result)
  elif len(diff_len) != 0:
    where = 'AND id in {}'.format(diff_len)
    result   = mariaDB.get_mariadb('*', where)
    response = kintone.post_kintone(result)
  elif len(diff_len) == 1:
    where  = where[:-2] + ')'
    result   = mariaDB.get_mariadb('*', where)
    response = kintone.post_kintone(result)

  #kintoneとDBの差分をチェック
  if set(maria) == set(kin_id) and not diff_list_tuple:
    print('no changes')
    sys.exit()

  return response

if __name__ == "__main__":
  response = check_record()

  if response.status_code == 200:
      print('success')
  else:
      print(response.text)