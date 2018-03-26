import sqlite3
import pandas as pd

timeFrames = ['2015-01']

# This shall create the data to train from the database,
# by getting relevant samples of parent post to commenter reply
for timeFrame in timeFrames:
    connection = sqlite3.connect('{}.db'.format(timeFrame))
    c= connection.cursor() #establish connection
    limit = 5000 #amount of sampling
    last_unix = 0
    cur_length = limit
    counter = 0
    test_done = False
    while cur_length == limit: #read loop till eof
        #get according to timestamp of comment, last unix will hold the case of last chained unix
        df = pd.read.sql("SELECT * FROM parent_reply WHERE unix > {} and parent NOT NULL and score > 0 ORDER BY unix ASC LIMIT {}".format(last_unix,limit),connection)
        last_unix = df.tail(1) ['unix'].values[0] #latest value
        cur_length = len(df)
        if not test_done:
            with open("test.from", 'a', encoding= 'utf8') as f:
                for content in df['parent'].values:
                    f.write(content + 'n') # write content
            with open("test.from", 'a', encoding='utf8') as f:
                for content in df['comment'].values:
                    f.write(content + 'n')  # write content
            test_done= True #end loop
        else:
            with open("test.to", 'a', encoding='utf8') as f:
                for content in df['parent'].values:
                    f.write(content + 'n')  # write content
            with open("test.to", 'a', encoding='utf8') as f:
                for content in df['comment'].values:
                    f.write(content + 'n')  # write content
        counter +=1
        if counter %20 == 0:
            print(counter * limit, 'rows done atm' )
