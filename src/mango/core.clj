(ns mango.core
  (:refer-clojure :exclude [count find] :rename {first core-first})
  (:use [monger.conversion :only [to-object-id]])
  (:require [monger.collection :as monger]
            [validateur.validation :as validateur]
            [monger db core joda-time]))

;Connection
(def connect!
  "Alias for monger.core/connect!"
  monger.core/connect!)

(def db!
  "Composes monger.core/set-db! to monger.core/get-db"
  (comp monger.core/set-db! monger.core/get-db))

(def connect-with-db!
  "Calls connect! and db! then returns a map containing :conn and :db keys."
  (comp (partial hash-map :conn (connect!) :db) db!))

(def disconnect!
  "Alias for monger.core/disconnect!"
  monger.core/disconnect!)

;DB
(def get-database-names
  "Alias for monger.core/get-db-names"
  monger.core/get-db-names)

(def get-collection-names
  "Alias for monger.db/get-collection-names"
  monger.db/get-collection-names)

(def drop-db
  "Alias for monger.db/drop-db"
  monger.db/drop-db)

;Validateurs
(defn wrap
  "Allows to create easily a new validateur.
  Argument must be a function."
  [f]
  #(fn [attribute] (apply f attribute %&)))

(def ^:dynamic *validateurs*
  "All the validateurs functions.
  You may rebind *validateurs* easily with with-validateurs."
  {:length (wrap #(apply validateur/length-of %1 :is %&))
   :length-within (wrap #(apply validateur/length-of %1 :within (range %2 (inc %3)) %&))
   :accepted (wrap validateur/acceptance-of)
   :excluded (wrap #(apply validateur/exclusion-of %1 :in %&))
   :included (wrap #(apply validateur/inclusion-of %1 :in %&))
   :formated (wrap #(apply validateur/format-of %1 :format %&))
   :numericality (wrap validateur/numericality-of)
   :required (wrap validateur/presence-of)
   :email (wrap #(apply validateur/format-of %1
     :format #"(?i)^[a-z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-z0-9-]+(?:\.[a-z0-9-]+)*$" %&))
   :url (wrap #(apply validateur/format-of %1
     :format #"(?ix)^(http|https):\/\/[a-z0-9]+([\-\.]{1}[a-z0-9]+)*\.[a-z]{2,5}(([0-9]{1,5})?\/.*)?$" %&))})

(defmacro with-validateurs
  "Rebings easily the *validateur* variable.
  (with-validateurs {:foo validateur-function}
    (defcollection bar (attr :foo)))"
  [validateurs & body]
  `(binding [*validateurs* (merge *validateurs* ~validateurs)]
     ~@body))

;Collection
(defprotocol Searchable
  (all [this])
  (find [this conds])
  (find-one [this conds])
  (find-by-id [this id])
  (first [this])
  ;as all records implement by default clojure.lang.IPersistentMap and its (count [this]) function
  ;count is named count-all here
  (count-all [this])
  (count [this conds]))

(defprotocol Writable
  (save [this document])
  (insert [this document])
  (update [this conds document])
  (update-by-id [this id document]))

(defprotocol Deletable
  (delete-all [this])
  (delete [this conds])
  (delete-by-id [this id]))

(defprotocol Validable
  (valid [this document])
  (valid? [this document]))

(defprotocol Buildable
  (build [this document]))

(defrecord Collection [name attributes types validations]
  Searchable
  (all [this] (find this {}))
  (find [this conds] (monger/find-maps name conds))
  (find-one [this conds] (monger/find-one-as-map name conds))
  (find-by-id [this id] (monger/find-map-by-id name (to-object-id id)))
  (first [this] (find-one this {}))
  (count-all [this] (monger/count name))
  (count [this conds] (monger/count name conds))

  Writable
  ;add :created_at and :updated_at keys?
  (save [this document]
    (if (get document :_id)
      (monger/save-and-return name (build this document))
      (insert this (build this document))))
  (insert [this document] (monger/insert-and-return name (build this document)))
  (update [this conds document] (monger/update name conds (build this document)))
  (update-by-id [this id document] (monger/update-by-id name (to-object-id id) (build this document)))

  Deletable
  (delete-all [this] (monger/remove name))
  (delete [this conds] (monger/remove name conds))
  (delete-by-id [this id] (monger/remove-by-id name (to-object-id id)))

  Validable
  (valid [this document] ((apply validateur/validation-set validations) document))
  (valid? [this document] (empty? (valid this document)))

  Buildable
  ;be smarter:
  (build [this document]
    (let [document (select-keys document attributes)]
      (reduce (fn [agg [attribute value]]
        (if-let [type (types attribute)]
          (let [value (case type
                          Boolean (if (= value "true") true false)
                          Long (Long/parseLong value)
                          Integer (Integer/parseInt value)
                          Float (Float/parseFloat value)
                          Double (Double/parseDouble value)
                          String value)]
            (assoc agg attribute value))
          (assoc agg attribute value))) {} document))))

(defmacro defcollection
  "Defines a new Collection.
  (defcollection users
    (username [:length-within 6 255])
    (password [:length-within 6 255])
    (email :email [:length-within 6 255])
    (^Boolean admin?))"
  [name & constraints]
  (letfn [(collection [constraints]
            {:attributes (cons :_id (keys constraints))
             :validations (for [[attr validations] constraints
                                validation validations]
                            (condp apply [validation]
                              vector? ((apply ((core-first validation) *validateurs*) (rest validation)) attr)
                              keyword? (((validation *validateurs*)) attr)))})]
    `(let [{attributes# :attributes validations# :validations}
           ('~collection (reduce (fn [agg# [attr# & vs#]] (assoc agg# (keyword attr#) vs#)) {} '~constraints))
           ;be smarter:
           types# (apply merge {} (map (juxt keyword (comp :tag meta)) (filter (comp :tag meta) (map core-first '~constraints))))]
       (def ~name (Collection. ~(str name) attributes# types# validations#)))))
