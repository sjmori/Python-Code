#!/usr/bin/python
# -*- coding: utf-8 -*-

#THIS IS A REVISED CODE VERSION OF THE BELOW CODE ON DEDUPE SITE WHICH WAS ORIGINALLY WRITTEN FOR MYSQL CONVERTED TO SQL SERVER
#AND WINDOWS ENVIRONMENT
#STEVE MORRIS
"""


__Note:__ You will need to run `python examples/mysql_example/mysql_init_db.py` 
before running this script. See the annotates source for 
[mysql_init_db](http://open-city.github.com/dedupe/doc/mysql_init_db.html)

For smaller datasets (<10,000), see our
[csv_example](http://open-city.github.com/dedupe/doc/csv_example.html)




"""
import os
import itertools
import time
import logging
import optparse
import locale
import pickle
import multiprocessing
import sys
from os import getenv


import pymssql
#import sexmachine.detector as gender

import dedupe

# ## Logging

# Dedupe uses Python logging to show or suppress verbose output. Added
# for convenience.  To enable verbose output, run `python
# example in SQL SERVER

def main():
    reload(sys)
    sys.setdefaultencoding("utf-8")
    optp = optparse.OptionParser()
    optp.add_option('-v', '--verbose', dest='verbose', action='count',
                    help='Increase verbosity (specify multiple times for more)'
                    )
    (opts, args) = optp.parse_args()
    log_level = logging.WARNING
    if opts.verbose == 1:
        log_level = logging.INFO
    elif opts.verbose >= 2:
        log_level = logging.DEBUG
    logging.getLogger().setLevel(log_level)

  

    settings_file = 'crm_person_deduping_settings'
    training_file = 'crm_person_deduping_training.json'

    start_time = time.time()



    server = "name_of_server"
    user = "name_of_user"
    password = "password"
    con = pymssql.connect(server,user,password,"ew_DataMatching", as_dict=True)
    con.autocommit(True)
    c = con.cursor()

    con2 = pymssql.connect(server,user,password,"ew_DataMatching")

    c2 = con2.cursor()
    # c2.execute("SET net_write_timeout = 3600")






    COMPANY_SELECT = "SELECT * from [vr_unmatched_person_remainder]" \





    # ## Training
	
	        # Define the fields dedupe will pay attention to
  

    if os.path.exists(settings_file):
        print 'reading from ', settings_file
        with open(settings_file) as sf :
            deduper = dedupe.StaticDedupe(sf, num_cores=4)
    else:

        fields = [
            {'field' : 'company_id','variable name' : 'company', 'type': 'Exact'}
            ,
        {'field' : 'fullname', 'variable name' : 'fullname','type': 'Text','has missing':'True'},
     {'field' : 'lname', 'variable name' : 'Surname',
                   'type' : 'String'},
                  {'field' : 'initial',
                   'type' : 'Exact', 'Has Missing' : True},
                  {'field' : 'gender',
                   'type' : 'Exact', 'Has Missing' : True}
            ,
              {'type' : 'Interaction',
                 'interaction variables' : ['fullname', 'company']},




                 # {'type' : 'Interaction',
                  # 'interaction variables' : ['AbbrFullName', 'Gender']},
                  {'type' : 'Interaction',
                  'interaction variables' : ['Surname', 'company']}

        ]


        # Create a new deduper object and pass our data model to it.
        deduper = dedupe.Dedupe(fields, num_cores=4)

        # We will sample pairs from the entire donor table for training
        c.execute(COMPANY_SELECT)
        temp_d = dict((i, row) for i, row in enumerate(c))

        print temp_d

        # -*- coding: utf-8 -*-

        deduper.sample(temp_d,11000)
        del temp_d

        # If we have training data saved from a previous run of dedupe,
        # look for it an load it in.
        #
        # __Note:__ if you want to train from
        # scratch, delete the training_file
        if os.path.exists(training_file):
            print 'reading labeled examples from ', training_file
            with open(training_file) as tf :
                deduper.readTraining(tf)

        # ## Active learning

        print 'starting active labeling...'
        # Starts the training loop. Dedupe will find the next pair of records
        # it is least certain about and ask you to label them as duplicates
        # or not.

        # use 'y', 'n' and 'u' keys to flag duplicates
        # press 'f' when you are finished
        dedupe.convenience.consoleLabel(deduper)

        # Notice our two arguments here
        #
        # `ppc` limits the Proportion of Pairs Covered that we allow a
        # predicate to cover. If a predicate puts together a fraction of
        # possible pairs greater than the ppc, that predicate will be removed
        # from consideration. As the size of the data increases, the user
        # will generally want to reduce ppc.
        #
        # `uncovered_dupes` is the number of true dupes pairs in our training
        # data that we are willing to accept will never be put into any
        # block. If true duplicates are never in the same block, we will never
        # compare them, and may never declare them to be duplicates.
        #
        # However, requiring that we cover every single true dupe pair may
        # mean that we have to use blocks that put together many, many
        # distinct pairs that we'll have to expensively, compare as well.
        deduper.train(ppc=0.01, uncovered_dupes=5)

        # When finished, save our labeled, training pairs to disk
        with open(training_file, 'w') as tf:
            deduper.writeTraining(tf)
        with open(settings_file, 'w') as sf:
            deduper.writeSettings(sf)

        # We can now remove some of the memory hobbing objects we used
        # for training
        deduper.cleanupTraining()

    ## Blocking

    print 'blocking...'

    # To run blocking on such a large set of data, we create a separate table
    # that contains blocking keys and record ids
    print 'creating blocking_map database'
    c.execute("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'ew_DataMatching.dedupe.blocking_map'))"
   "DROP TABLE ew_DataMatching.dedupe.blocking_map")
    c.execute("CREATE TABLE ew_DataMatching.dedupe.blocking_map "
              "(block_key VARCHAR(200), matching_id INT)"
              )


    # If dedupe learned a TF-IDF blocking rule, we have to take a pass
    # through the data and create TF-IDF canopies. This can take up to an
    # hour
    print 'creating inverted index'


    for field in deduper.blocker.index_fields  :


        c2.execute("SELECT  %s FROM [vr_unmatched_person_remainder]" % field)
        field_data = (row[0] for row in c2)


        deduper.blocker.index(field_data, field)

    # Now we are ready to write our blocking map table by creating a
    # generator that yields unique `(block_key, donor_id)` tuples.
    print 'writing blocking map'

    c.execute(COMPANY_SELECT)


    full_data = ((row['matching_id'], row) for row in c)

   # for line in full_data:
   #       id_number,data = line
   #       if data['gender'] == '':
    #            print id_number

    b_data = deduper.blocker(full_data)



    def dbWriter(sql, rows) :
        conn  = pymssql.connect(server,user,password,"ew_DataMatching")


        cursor = conn.cursor()
        cursor.executemany(sql, rows)
        cursor.close()
        conn.commit()
        conn.close()
    results = dbWriter("INSERT INTO ew_DataMatching.dedupe.blocking_map VALUES (%s, %s)",b_data)

    print 'prepare blocking table. this will probably take a while ...'

    logging.info("indexing block_key")
    c.execute("CREATE INDEX blocking_map_key_idx ON dedupe.blocking_map (block_key)")

    c.execute("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'ew_DataMatching.dedupe.[plural_key]')) DROP TABLE ew_DataMatching.dedupe.[plural_key]")
    c.execute("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'ew_DataMatching.dedupe.[plural_block]')) DROP TABLE ew_DataMatching.dedupe.[plural_block]")
    c.execute("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'ew_DataMatching.dedupe.[covered_blocks]')) DROP TABLE ew_DataMatching.dedupe.[covered_blocks]")
    c.execute("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'ew_DataMatching.dedupe.[smaller_coverage]')) DROP TABLE ew_DataMatching.dedupe.[smaller_coverage]")

    # Many block_keys will only form blocks that contain a single
    # record. Since there are no comparisons possible withing such a
    # singleton block we can ignore them.
    logging.info("calculating plural_key")
    c.execute("CREATE TABLE ew_DataMatching.dedupe.plural_key "
              "(block_key VARCHAR(200), "
              " block_id INT identity(1,1), "
              " PRIMARY KEY (block_id)) ")

    c.execute("INSERT INTO dedupe.plural_key(block_key) SELECT block_key FROM ew_DataMatching.dedupe.blocking_map "
               "GROUP BY block_key HAVING COUNT(*) > 1")

    logging.info("creating block_key index")
    c.execute("CREATE UNIQUE INDEX block_key_idx ON ew_DataMatching.dedupe.plural_key (block_key)")

    logging.info("calculating plural_block")
    c.execute("SELECT distinct block_id, matching_id into dedupe.plural_block"
              " FROM ew_DataMatching.dedupe.blocking_map INNER JOIN ew_DataMatching.dedupe.plural_key "
              " on blocking_map.block_key = plural_key.block_key order by block_id")

    logging.info("adding donor_id index and sorting index")
    c.execute("CREATE INDEX matching_id ON dedupe.plural_block (matching_id)")
    c.execute("CREATE UNIQUE INDEX idx_matching_id_2 on dedupe.plural_block (block_id, matching_id)")

    # To use Kolb, et.al's Redundant Free Comparison scheme, we need to
    # keep track of all the block_ids that are associated with a
    # particular donor records. Original code had group_concat function from MySQL, below code replaces this for SQL Server


    logging.info("creating covered_blocks")
    c.execute("SELECT matching_id,"
               "dbo.trim_character(CAST(("
               "SELECT  cast(block_id as varchar)+',' "
               "FROM ew_DataMatching.dedupe.plural_block T WHERE plural_block.matching_id = T.matching_id order by block_id FOR XML PATH(''))as varchar(max)),',') AS sorted_ids into dedupe.covered_blocks FROM ew_DataMatching.dedupe.plural_block "
               "GROUP BY matching_id")

    c.execute("CREATE UNIQUE INDEX donor_idx ON ew_DataMatching.dedupe.covered_blocks (matching_id)")

    # In particular, for every block of records, we need to keep
    # track of a donor records's associated block_ids that are SMALLER than
    # the current block's id. Code converted for SQL SERVER
    logging.info("creating smaller_coverage")
    c.execute("SELECT plural_block.matching_id, block_id, "
              "case when charindex(cast(block_id as varchar),sorted_ids,1) = 1 then null else substring(sorted_ids,1,charindex(cast(block_id as varchar),sorted_ids,1)-2) end"
              " AS smaller_ids INTO dedupe.smaller_coverage"
              " FROM ew_DataMatching.dedupe.plural_block INNER JOIN ew_DataMatching.dedupe.covered_blocks "
              " on plural_block.matching_id = covered_blocks.matching_id order by matching_id,block_id")

    con.commit()


    ## Clustering

    def candidates_gen(result_set) :
        lset = set

        block_id = None
        records = []
        i = 0
        for row in result_set :
            if row['block_id'] != block_id :
                if records :
                    yield records

                block_id = row['block_id']
                records = []
                i += 1

                if i % 10000 == 0 :
                    print i, "blocks"
                    print time.time() - start_time, "seconds"

            smaller_ids = row['smaller_ids']

            if smaller_ids :
                smaller_ids = lset(smaller_ids.split(','))



            else :
                smaller_ids = lset([])

            records.append((row['matching_id'], row, smaller_ids))
            #print records


        if records :
            yield records

    c.execute("SELECT MasterMatching.matching_id, fullname,gender, "
              " initial,lname,company_id,"


              " block_id, smaller_ids "
              "FROM dedupe.smaller_coverage "
              "INNER JOIN [vr_unmatched_person_remainder]  MasterMatching"
              " on MasterMatching.matching_id = smaller_coverage.matching_id order by block_id,matching_id")
   # for row in c:
      #  if row["company"] == 'Cheshunt School & Technology College':
       #     print('row = %r' % (row,))

    print 'clustering...'

#0.5 Default

    clustered_dupes = deduper.matchBlocks(candidates_gen(c),
                                          threshold=0.5)

    ## Writing out results

    # We now have a sequence of tuples of donor ids that dedupe believes
    # all refer to the same entity. We write this out onto an entity map
    # table
    c.execute("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'ew_DataMatching.dedupe.[company_entity_map]')) DROP TABLE ew_DataMatching.dedupe.[company_entity_map]")

    print 'creating entity_map database'
    c.execute("CREATE TABLE ew_DataMatching.dedupe.company_entity_map "
              "(matching_id INTEGER, canon_id INTEGER, score FLOAT"
              "  PRIMARY KEY(matching_id))")

    for cluster, score in clustered_dupes :
        cluster_id = cluster[0]

        for matching_id, score in zip(cluster, score) :
            #print cluster_id, matching_id
            c.execute('INSERT INTO ew_DataMatching.dedupe.company_entity_map (matching_id, canon_id, score) VALUES (%s, %s , %s)',

                      (int(matching_id), int(cluster_id),round(score,2)

                      ))

    con.commit()

    c.execute("CREATE INDEX head_index ON ew_DataMatching.dedupe.company_entity_map (canon_id)")
    con.commit()

    # Print out the number of duplicates found
    print '# duplicate sets'
    print len(clustered_dupes)



    print 'ran in', time.time() - start_time, 'seconds'

if __name__ == "__main__":
	main()