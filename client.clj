#!/usr/bin/env bb

(require
 '[honey.sql :as sql]
 '[netpod.pods :as pods])

(def contact-ins
  {:insert-into :contacts
   :values [{:id :?id
             :first_name :?first_name
             :last_name :?last_name
             :email :?email}]})

(def all-q {:select [:*]
            :from [:contacts]})

(def signals-q
  {:select [[[:count "*"] :count] :sensor_name]
   :from [:signals]
   :group-by [:sensor_name]
   :order-by [[:count :desc]]})

(def domain-q {:select [:*]
               :from [:contacts]
               :where [:like :email :?domain]})

(defn to-sql
  "uses honeysql to generate an sql string"
  ([sql-map params]
   (-> (sql/format sql-map {:inline true
                            :params params})
       first))
  ([sql-map]
   (to-sql sql-map nil)))

(defn random-item
  [coll]
  (let [coll (vec coll)
        size (count coll)
        idx (rand-int size)]
    (get coll idx)))

(defn random-signal
  []
  (let [uuid (str (random-uuid))
        sensor-name (random-item ["wheezy" "chewy" "poky" "smooth" "chill" "touchy"])
        gauge (rand-int 20000)]
    (vector uuid sensor-name gauge)))

;;before running do
;;cargo build --release
;;to build the binary
(pods/with-pod "./target/release/netpod-duckdb"
  ;; require is not suitable in macros
  ;; but one can also resolve things dynamically using resolve such as below
  (let [query (resolve 'netpod.duckdb/query)
        exec (resolve 'netpod.duckdb/exec)
        append (resolve 'netpod.duckdb/append)]
    (println @(exec "CREATE TABLE contacts(id BIGINT, first_name VARCHAR, last_name VARCHAR, email VARCHAR)"))
    (println @(exec (to-sql contact-ins {:id 1
                                         :first_name "John"
                                         :last_name "Doe"
                                         :email "john@doe.com"})))
    (println @(exec (to-sql contact-ins {:id 2
                                         :first_name "Pete"
                                         :last_name "Stolli"
                                         :email "petes@gmail.com"})))
    (println @(exec (to-sql contact-ins {:id 3
                                         :first_name "Mike"
                                         :last_name "Freight"
                                         :email "mf@gmail.com"})))
    (println @(query (to-sql domain-q {:domain "%@test.com"})))
    (println @(query (to-sql domain-q {:domain "%@gmail.com"})))
    (println @(query (to-sql domain-q {:domain "%@yahoo.com"})))
    (println @(query (to-sql domain-q {:domain "%@doe.com"})))
    (println @(query (to-sql domain-q {:domain "%@hotmail.com"})))
    (println @(exec "CREATE TABLE signals (signal_id UUID, sensor_name VARCHAR(255), gauge INT)"))
    (println @(append "signals" (vec (repeatedly 1000 random-signal))))
    (println @(query (to-sql signals-q)))
    (println @(append "signals" (vec (repeatedly 10000 random-signal))))
    (println @(query (to-sql signals-q)))
    (println @(append "signals" (vec (repeatedly 1000000 random-signal))))
    (println @(query (to-sql signals-q)))
    (println @(exec "DROP TABLE contacts"))
    (println @(exec "DROP TABLE signals"))))

