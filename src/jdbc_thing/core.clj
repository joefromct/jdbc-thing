(ns jdbc-thing.core 
  (:use 
        [clojure.java.jdbc :exclude [resultset-seq]]
        [clojure.pprint ]
        [incanter.core]
        ))

(defn- init-default-connections 
  "When a connections file or directory isn't present, this will default to
  something in order to test/etc. (should be the h2db example connection) "
  [] 
  (let [
        connections-file (clojure.java.io/file (System/getProperty "user.home") "/.jdbc-thing/connections2" )  
        connections-dir  (clojure.java.io/file (.getParent connections-file) ) 
        ]  
    ; The default persistent array map for connections to be spit to the file 
    (def connections {
                      :pgdb {
                             :classname   "org.postgresql.Driver" 
                             :project-dependency "postgresql/postgresql \"9.1-901.jdbc4\" ] "
                             :subprotocol "postgresql"
                             :subname     "//localhost:5432/stuff"
                             :user     "postgres"
                             :password "postgres"} 
                      :mssqldb {
                                :classname   "net.sourceforge.jtds.jdbc.Driver" 
                                :project-dependency "[net.sourceforge.jtds/jtds \"1.2.4\"] "
                                :subprotocol "jtds:sqlserver"
                                :subname "//hostname.domain.net/DATABASE;INSTANCE=db1042"   
                                :user     "username" 
                                :password "password"}
                      :as400db {
                                :classname "com.ibm.as400.access.AS400JDBCDriver"
                                :project-dependency "[net.sf.jt400/jt400  \"6.7\" ] "
                                :subprotocol "as400" 
                                :subname "//somehost.domain.com:446/userlib"  
                                :user     "username"
                                :password "userpassword"
                                :description "description" }
                      :h2db-example {:classname   "org.h2.Driver" 
                             :project-dependency "[com.h2database/h2 \"1.3.170\" ]  "
                             :subprotocol "h2"
                             :subname "mem:test;DB_CLOSE_DELAY=-1"  
                             ;:subname (str "file://c:/bin/Sample23.h2.db;DB_CLOSE_DELAY=-1" )
                             :user     "sa"
                             :password ""}
                      }  
      ) 
    (if (false? (.isDirectory connections-dir) ) 
      (.mkdir  connections-dir )) 
    (spit (clojure.java.io/file (System/getProperty "user.home") "/.jdbc-thing/connections" ) (with-out-str (pprint connections) ) ) 
    ) 
  )

(defn get-connections
  "list the buffers available in the ~/.jdbc-connections/connections file" 
  []
  (doseq  [
           [k v] 
           (with-in-str 
             (slurp 
               (clojure.java.io/file 
                 (System/getProperty "user.home") 
                  "/.jdbc-thing/connections") ) (read)  )
           ]  (prn k  ) )
  )

(defn- get-db-connection-map   
  "We slurp the home directory connections file, find the match  for
  the passed in db-spec, and return the proper mapped database connection.
  TODO: Need to test what happens when this directory/file doesn't exist... can
  I somehow help the user create this file? "
  [ db-spec ]
  {:post ; We have to hand back something to try to connect to 
        [
         (not (nil? connection-map))
         ] }
  (def connection-map (db-spec (with-in-str 
             (slurp 
               (clojure.java.io/file 
                 (System/getProperty "user.home") "/.jdbc-thing/connections") )
                  (read)))) 
    connection-map
)

(defn- db-fetch-results 
  "Treat lazy result sets in whole for returning a database query"  
  [db-spec query] 
  (with-connection 
    (get-db-connection-map db-spec) 
    (with-query-results res query 
      ;(pprint (type res) ) 
      (doall res)))
  )


(defn- db-print-table
  "Alpha - subject to change.
   Prints a collection of maps in a textual table. Prints table headings
   ks, and then a line of output for each row, corresponding to the keys
   in ks. If ks are not specified, use the keys of the first item in rows."
  ([ks rows]
    ;(pprint (type rows )  ) 
     (when (seq rows)
       (let [
             widths (map
                     (fn [k]
                       (apply max 
                              (count (str k)) 
                              (map #(count (
                                            ; \r\n is seen as two chars by the
                                            ; OS, but we are representing it as
                                            ; 6 chars in \\r\\n in the output: 
                                            clojure.string/replace (str (get % k)) "\r\n" "      " 
                                            )) rows)))
                     ks)
             fmts (map #(str "%-" % "s") widths)
             fmt-row (fn [row]
                       (apply str 
                              (interpose " | "
                                             (for [[col fmt] (map vector (map #(get row %) ks) fmts)]
                                                  (format fmt (str col)))
                                             )
                              )
                       )
             header (fmt-row (zipmap ks ks))
             bar (apply str (repeat (count header) "="))
             ]
         (println bar)
         (println header)
         (println bar)
         (doseq [row rows]
           ; TODO we will have to find the newline char of the OS and then replace
           ; with the correct thing here.  
            (println (clojure.string/replace (fmt-row row) "\r\n" "\\r\\n" ) )
           )
         (println bar))))
  ;for airity without column headings 
  ([rows] (db-print-table (keys (first rows)) rows))
  )


(spit 
  (clojure.java.io/file (System/getProperty "user.home") "/.jdbc-thing/temp" ) 
  (with-out-str (db-print-table (
                                  db-fetch-results 
                                  :mssqldb 
                                 ["select top 100 getdate()  "] 
                                 )  
                                )
                ) 
  ) 

(defn- db-get-table-metadata 
  "Uses DatabaseMetaData getColumns to return useful information on the
  metadata for a table passed in, returns useful details based on the specified
  table/column pairings.

  From here: 
    http://docs.oracle.com/javase/1.4.2/docs/api/java/sql/DatabaseMetaData.html#getColumns(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
  Example usage: 
    (get-table-metadata {
                        :db-spec :pgdb 
                        :tablenamepattern \"keirsey_instance_detail\"} 
                        :return-meta-columns [:table_name :column_name :type_name ] 
                       ) 
  "
  [ 
   {:keys [ db-spec catalog schemapattern tablenamepattern return-meta-columns ] 
    :or {
         catalog nil 
         schemapattern nil 
         tablenamepattern nil 
         return-meta-columns [ ; TODO Should the below somehow be metadata? 
                              :table_cat        ; table catalog (may be null)
                              :table_schem      ; table schema (may be null)
                              :table_name       ; table name
                              :column_name      ; column name
                              :type_name        ; Data source dependent type name, for a UDT the type name is fully qualified
                              :column_size      ; column size. For char or date types this is the maximum number of characters, for numeric or decimal types this is precision.
                              :remarks          ; comment describing column (may be null)
                              :column_def       ; default value (may be null)
                              :ordinal_position ; index of column in table (starting at 1)
                              ;:data_type        ; SQL type from java.sql.Types
                              ;:decimal_digits   ; the number of fractional digits
                              ;:nullable         ; is NULL allowed.
                              ] }  }
   ] 
  {:pre [
         (not (nil? db-spec))   ; Have to have a db-spec to connect to 
         (not (nil? tablenamepattern ))  ; we don't want to return every table on a large database 
         ]}
  (
   (fn [ ]
     (clojure.java.jdbc/with-connection (get-db-connection-map db-spec )  
       (map #(select-keys 
               %   
               return-meta-columns 
               )
                (into #{}
                  (result-set-seq (->
                                    (clojure.java.jdbc/connection )
                                    (.getMetaData)
                                    (.getColumns catalog schemapattern tablenamepattern "%" )
                                    ;(.getColumns "stuff" "public" nil "%")
                                    )
                                  )
                  )
            )
       )
     )
   )
)


(defn broker-request 
    "This is the broker function to send things to the database.  at some point
    it should have some sort of returned issued even if the request isn't
    carried out (for long running queries) which can act as a light weight
    threading (as editors/vim might not be great at that.)  " 
    [ {:keys [db-spec request-type database-code  ], 
       :or {request-type "select"}  } ]
  {:pre [ 
         (not (nil? db-spec) ) 
         (not (nil? database-code) ) 
         ] 
   }
    (cond 
      (= request-type "select"        ) (db-fetch-results db-spec [database-code] ) 
      (= request-type "drop"          ) (db-do-commands (get-db-connection-map db-spec) false database-code ) 
      (= request-type "create"        ) (db-do-commands (get-db-connection-map db-spec) false database-code ) 
      (= request-type "insert"        ) (println "insert")   
      (= request-type "delete"        ) (println "delete") 
      (= request-type "describe-table") (println "describe table") 
      :else "something else???" 
  )
)

; would like to sort of setup some atoms so that this works with defaults that are per-file/buffer.  
(broker-request {
                  :db-spec :mssqldb 
                  :request-type "drop" 
                  :database-code "drop table dbo.jr_temp" })

(broker-request {
                 :db-spec :mssqldb 
                 :request-type "create" 
                 :database-code "create table dbo.jr_temp ( temp varchar(100) ) " 
                 })



;(.. ResultSetMetaData (getMetaData) )
;ResultSetMetaData rsmd = resultSet.getMetaData();
;String column = rsmd.getColumnName(index);


;(pprint (send-request {:connection-buffer :pgdb 
;               :database-code "select * from pg_tables limit 10 "
;               :request-type "select" }) ) 

;(pprint (get-db-metadata :pgdb ))
;
;
 
(defn statement-get-max-field-size  
  "utilizes java Statement.getMaxFiledSize() to fetch the field sizes back into
  a sequence for subsequent processing."
  [ params ]
  (println "first-expression") 
)

(defn print-fixed-width  
  "Recieves a mapped result set from jdbc and a list of rpadding values.
  Returns a list of strings that resembles the output to be displayed in text,
  with headers first, then horizontal rule, then each value in the result set
  appended with the time the query took to run."
  [ results , rpad-list  ]
  (println "first-expression") 
)

