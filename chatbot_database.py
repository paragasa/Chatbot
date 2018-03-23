import _sqlite3
import json
from datetime import datetime

#PURPOSE GET TRAINING DATA FOR NEXT MODEL


#connections
timeFrame = '2015-05'
sql_transaction = []
connection = _sqlite3.connect('{}.db'.format(timeFrame))
c = connection.cursor()

def create_table():  #data table to hold reddit variables
    c.execute("""CREATE TABLE IF NOT EXISTS parent_reply(parent_id TEXT PRIMARY KEY,
    comment_id TEXT UNIQUE, parent TEXT, comment TEXT, subreddit TEXT, unix INT,
     score INT)""")

if __name__ == "__main__":
    create_table()
    row_counter = 0 # how many sift rows
    paired_rows = 0  # pairs made, case of parent comment matching child comment

    #format body
    def format_data(data):
        data = data.replace('\n', 'newlinechar').replace('\r', 'newlinechar').replace('"', "'")
        return data
    #end format

    def find_parent(pid):
        try: #find childs parent id
            sql= "SELECT comment FROM parent_reply WHERE comment_id == '{}' LIMIT 1".format(pid)
            c.execute(sql)
            result = c.fetchone()
            if result != None: #catch
                return result[0]
            else: return False
        except Exception as e:
            #print (str(e))
             return False


    def find_existing_score(pid):
        try: #find childs parent id
            sql= "SELECT score FROM parent_reply WHERE parent_id == '{}' LIMIT 1".format(pid)
            c.execute(sql)
            result = c.fetchone()
            if result != None: #catch
                return result[0]
            else: return False
        except Exception as e:
            #print (str(e))
             return False

    def acceptable (data): #returns data case where data is valid, not deleted, removed, excessive
        if len(data.split(' ')) > 50 or len(data)<1:
            return False
        elif len(data)> 1000:
            return False
        elif data == '[deleted]':
            return False
        elif data == '[removed]':
            return False
    def transaction_builder(sql):
        global sql_transaction
        sql_transaction.append(sql) #pass statement
        if len(sql_transaction)> 1000: # want to check length to catch
            c.execute('BEGIN TRANSACTION') #begin load
            for s in sql_transaction: #load transactions
                try:
                    c.execute(s)
                except:
                    pass
            connection.commit()#commit to database
            sql_transaction = [] #saved so remove transaction

    #replace popular comment reply if read
    def sql_insert_replace_comment( commentid, parentid, parent,comment, subreddit, time, score ):
        try:
            sql = """UPDATE parent_reply SET parent_id = ?, comment_id = ?, parent = ?, comment = ?, 
            subreddit = ?, unix = ?, score = ? WHERE parent_id =?;""".format(
                parentid, commentid, parent, comment, subreddit, int(time), score, parentid)
            transaction_builder(sql)
        except Exception as e:
            print('replace_comment', str(e))
  #insert if not higher score and has parent
    def sql_insert_has_parent( commentid, parentid, parent,comment, subreddit, time, score ):
        try:
            sql = """INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, unix, score)
                    VALUES ("{}","{}","{}","{}","{}",{},{});""".format(
                    parentid, commentid, parent, comment, subreddit, int(time), score)
            transaction_builder(sql)
        except Exception as e:
            print('insert_comment', str(e))
 #case where no parent but still retain comment ( delete case?)
    def sql_insert_no_parent(commentid, parentid, comment, subreddit, time, score):
        try:
            sql = """INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score) 
            VALUES ("{}","{}","{}","{}",{},{});""".format(
                parentid, commentid, comment, subreddit, int(time), score)
            transaction_builder(sql)
        except Exception as e:
            print('insert_comment_no_parent', str(e))
    #read file line at a time
    with open('/Users/paragasa/Downloads/{}/RC_{}'.format(timeFrame.split('-')[0],timeFrame), buffering=1000) as f:
        for row in f:
            #print(row)
            row_counter +=1
            row = json.loads(row)
            parent_id = row['parent_id']
            body = format_data(row['body'])
            created_utc = row ['created_tuc']
            score = row ['score']
            comment_id = row['name']
            subreddit = row ['subreddit']
            parent_data = find_parent(parent_id)


            if score>= 2: # get case where reply happened
                if acceptable(body): #check format
                    existing_comment_score = find_existing_score(parent_id)
                    if existing_comment_score:
                        if score > existing_comment_score: #must be better response, updates row
                            sql_insert_replace_comment(comment_id, parent_id, parent_data,body, subreddit, created_utc, score )

                    else:
                        if parent_data:
                            sql_insert_has_parent(comment_id, parent_id, parent_data,body, subreddit, created_utc, score )
                            paired_rows +=1
                        else:
                            sql_insert_no_parent(comment_id, parent_id, parent_data,body, subreddit, created_utc, score )
            # print read rows w/ pairing
            if row_counter % 100000 == 0:
             print('Total Rows Read: {}, Paired Rows: {}, Time: {}'.format(row_counter, paired_rows,
                                                                              str(_datetime.now())))