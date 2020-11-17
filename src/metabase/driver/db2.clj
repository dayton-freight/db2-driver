(ns metabase.driver.db2
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [honeysql.core :as hsql]
            [metabase.driver :as driver]
            [metabase.driver.common :as driver.common]
            [metabase.driver.sql
             [query-processor :as sql.qp]
             [util :as sql.u]]
            [metabase.driver.sql-jdbc
             [connection :as sql-jdbc.conn]
             [execute :as sql-jdbc.execute]
             [sync :as sql-jdbc.sync]]
            [metabase.driver.sql.util.unprepare :as unprepare]
            [metabase.util
             [honeysql-extensions :as hx]
             [ssh :as ssh]])
  (:import [java.sql ResultSet Types]
           java.util.Date))

(driver/register! :db2, :parent :sql-jdbc)


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                             metabase.driver impls                                              |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod driver/display-name :db2 [_] "DB2")

(defmethod driver/humanize-connection-error-message :db2 [_ message]
  (condp re-matches message
    #"^FATAL: database \".*\" does not exist$"
    (driver.common/connection-error-messages :database-name-incorrect)

    #"^No suitable driver found for.*$"
    (driver.common/connection-error-messages :invalid-hostname)

    #"^Connection refused. Check that the hostname and port are correct and that the postmaster is accepting TCP/IP connections.$"
    (driver.common/connection-error-messages :cannot-connect-check-host-and-port)

    #"^FATAL: role \".*\" does not exist$"
    (driver.common/connection-error-messages :username-incorrect)

    #"^FATAL: password authentication failed for user.*$"
    (driver.common/connection-error-messages :password-incorrect)

    #"^FATAL: .*$" ; all other FATAL messages: strip off the 'FATAL' part, capitalize, and add a period
    (let [[_ message] (re-matches #"^FATAL: (.*$)" message)]
      (str (str/capitalize message) \.))

    #".*" ; default
    message))

(defmethod driver.common/current-db-time-date-formatters :db2 [_]     ;; "2019-09-14T18:03:20.679658000-00:00"
;;  (println "current-db-time-date-formatters")
  (mapcat
   driver.common/create-db-time-formatters
   ["yyyy-MM-dd HH:mm:ss"
    "yyyy-MM-dd HH:mm:ss.SSS"
    "yyyy-MM-dd'T'HH:mm:ss.SSS"
    "yyyy-MM-dd HH:mm:ss.SSSZ"
    "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
    "yyyy-MM-dd HH:mm:ss.SSSZZ"
    "yyyy-MM-dd'T'HH:mm:ss.SSSZZ"
    "yyyy-MM-dd HH:mm:ss.SSSSSSZZ"
    "yyyy-MM-dd'T'HH:mm:ss.SSSSSSZZ"
    "yyyy-MM-dd HH:mm:ss.SSSSSSSSSZZ"
    "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSZZ"]))

(defmethod driver.common/current-db-time-native-query :db2 [_]
;;  (println "current-db-time-native-query")
  "SELECT TO_CHAR(CURRENT TIMESTAMP, 'yyyy-MM-dd HH:mm:ss') FROM SYSIBM.SYSDUMMY1")       ;; "SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1")

(defmethod driver/current-db-time :db2 [& args]
;;  (println "current-db-time")
  (apply driver.common/current-db-time args))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                           metabase.driver.sql impls                                            |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn- date-format [format-str expr] (hsql/call :varchar_format expr (hx/literal format-str)))
(defn- str-to-date [format-str expr] (hsql/call :to_date expr (hx/literal format-str)))

(defn- trunc-with-format [format-str expr]
  (str-to-date format-str (date-format format-str expr)))

(defmethod sql.qp/date [:db2 :default]        [_ _ expr] expr)
(defmethod sql.qp/date [:db2 :minute]         [_ _ expr] (trunc-with-format "YYYY-MM-DD HH24:MI" expr))
(defmethod sql.qp/date [:db2 :minute-of-hour] [_ _ expr] (hsql/call :minute expr))
(defmethod sql.qp/date [:db2 :hour]           [_ _ expr] (trunc-with-format "YYYY-MM-DD HH24" expr))
(defmethod sql.qp/date [:db2 :hour-of-day]    [_ _ expr] (hsql/call :hour expr))
(defmethod sql.qp/date [:db2 :day]            [_ _ expr] (hsql/call :date expr))
(defmethod sql.qp/date [:db2 :day-of-month]   [_ _ expr] (hsql/call :day expr))
(defmethod sql.qp/date [:db2 :week]           [_ _ expr] (hx/- expr (hsql/raw (format "%d days" (int (hx/- (hsql/call :dayofweek expr) 1))))))
(defmethod sql.qp/date [:db2 :month]          [_ _ expr] (str-to-date "YYYY-MM-DD" (hx/concat (date-format "YYYY-MM" expr) (hx/literal "-01"))))
(defmethod sql.qp/date [:db2 :month-of-year]  [_ _ expr] (hsql/call :month expr))
(defmethod sql.qp/date [:db2 :quarter]        [_ _ expr] (str-to-date "YYYY-MM-DD" (hsql/raw (format "%d-%d-01" (int (hx/year expr)) (int ((hx/- (hx/* (hx/quarter expr) 3) 2)))))))
(defmethod sql.qp/date [:db2 :year]           [_ _ expr] (hsql/call :year expr))

(defmethod sql.qp/date [:db2 :day-of-year] [driver _ expr] (hsql/call :dayofyear expr))

(defmethod sql.qp/date [:db2 :week-of-year] [_ _ expr] (hsql/call :week expr))

(defmethod sql.qp/date [:db2 :quarter-of-year] [driver _ expr] (hsql/call :quarter expr))

(defmethod sql.qp/date [:db2 :day-of-week] [driver _ expr] (hsql/call :dayofweek expr))

(defmethod driver/date-add :db2 [_ dt amount unit]
  (hx/+ (hx/->timestamp dt) (case unit
                              :second  (hsql/raw (format "current timestamp + %d seconds" (int amount)))
                              :minute  (hsql/raw (format "current timestamp + %d minutes" (int amount)))
                              :hour    (hsql/raw (format "current timestamp + %d hours" (int amount)))
                              :day     (hsql/raw (format "current timestamp + %d days" (int amount)))
                              :week    (hsql/raw (format "current timestamp + %d days" (int (hx/* amount (hsql/raw 7)))))
                              :month   (hsql/raw (format "current timestamp + %d months" (int amount)))
                              :quarter (hsql/raw (format "current timestamp + %d months" (int (hx/* amount (hsql/raw 3)))))
                              :year    (hsql/raw (format "current timestamp + %d years" (int amount))))))

(defmethod sql.qp/unix-timestamp->timestamp [:db2 :seconds] [_ _ expr]
  (hx/+ (hsql/raw "timestamp('1970-01-01 00:00:00')") (hsql/raw (format "%d seconds" (int expr))))

(defmethod sql.qp/unix-timestamp->timestamp [:db2 :milliseconds] [driver _ expr]
  (hx/+ (hsql/raw "timestamp('1970-01-01 00:00:00')") (hsql/raw (format "%d seconds" (int (hx// expr 1000)))))))

(def ^:private now (hsql/raw "current timestamp"))

(defmethod sql.qp/current-datetime-fn :db2 [_] now)


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                         metabase.driver.sql-jdbc impls                                         |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod sql-jdbc.conn/connection-details->spec :db2
  [_ {:keys [host port db]
      :or   {host "localhost", port 446, db ""}
      :as   details}]
  (merge
   {:classname   "com.ibm.as400.access.AS400JDBCDriver"
    :subprotocol "as400"
    :subname     (str "//" host
                      ":" port
                      "/" db)}
   (dissoc details :host :port :dbname)))

(defmethod driver/can-connect? :db2 [driver details]
  (let [connection (sql-jdbc.conn/connection-details->spec driver (ssh/include-ssh-tunnel details))]
    (= 1 (first (vals (first (jdbc/query connection ["VALUES 1"])))))))

(defmethod sql-jdbc.sync/database-type->base-type :db2 [_ database-type]
  ({:BIGINT       :type/BigInteger    ;; Mappings for DB2 types to Metabase types.
    :BINARY       :type/*             ;; See the list here: https://docs.tibco.com/pub/spc/4.0.0/doc/html/ibmdb2/ibmdb2_data_types.htm
    :BLOB         :type/*
    :BOOLEAN      :type/Boolean
    :CHAR         :type/Text
    :CLOB         :type/Text
    :DATALINK     :type/*
    :DATE         :type/Date
    :DBCLOB       :type/Text
    :DECIMAL      :type/Decimal
    :DECFLOAT     :type/Decimal
    :DOUBLE       :type/Float
    :FLOAT        :type/Float
    :GRAPHIC      :type/Text
    :INTEGER      :type/Integer
    :NUMERIC      :type/Decimal
    :REAL         :type/Float
    :ROWID        :type/*
    :SMALLINT     :type/Integer
    :TIME         :type/Time
    :TIMESTAMP    :type/DateTime
    :VARCHAR      :type/Text
    :VARGRAPHIC   :type/Text
    :XML          :type/Text
    (keyword "CHAR() FOR BIT DATA")       :type/*
    (keyword "LONG VARCHAR")              :type/*
    (keyword "LONG VARCHAR FOR BIT DATA") :type/*
    (keyword "LONG VARGRAPHIC")           :type/*
    (keyword "VARCHAR() FOR BIT DATA")    :type/*} database-type))

(defmethod sql-jdbc.sync/excluded-schemas :db2 [_]
  #{"SQLJ" 
    "SYSCAT" 
    "SYSFUN" 
    "SYSIBMADM" 
    "SYSIBMINTERNAL" 
    "SYSIBMTS" 
    "SPOOLMAIL"
    "SYSPROC" 
    "SYSPUBLIC" 
    "SYSSTAT"
    "SYSTOOLS"})

(defmethod sql-jdbc.execute/set-timezone-sql :db2 [_]
  "SET SESSION TIME ZONE = %s")