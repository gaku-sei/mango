(ns mango.core-test
  (:refer-clojure :exclude [count find first])
  (:require [clojure.test :refer :all]
            [mango.core :refer :all]))

(connect-with-db! "mango")

(defcollection users
  (username :required [:length-within 6 255])
  (password :required [:length-within 6 255])
  (email :required :email [:length-within 6 255])
  (website [:url :allow-nil true]))

(def valid-user
  {:username "foobar" :password "foobar" :email "foo@bar.baz" :website "http://www.foobar.baz"})

(deftest collection
  (testing "defcollection and validations"
    (are [x y] (= x y)
         (:name users) "users"
         (sort (:attributes users)) (sort [:username :password :email :website]))
    (is (valid? users valid-user))))
    ;(are [x] ((complement valid?) users x)
    ;     bad-users)

(drop-db)
(disconnect!)
