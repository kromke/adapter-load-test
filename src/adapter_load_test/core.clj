(ns adapter-load-test.core
  (:require [dvlopt.kafka       :as K]
            [dvlopt.kafka.out   :as K.out]
            [dvlopt.kafka.in   :as K.in]
            [clojure.data.json :as json])
  (:import [java.time Instant Duration])
  (:gen-class))

(defn- get-config [file] (read-string (slurp file)))

(defn- get-fn-requestId? [record]
  (fn [value] (contains?
               (set
                (map
                 #(some->> (::K/headers %)
                           (filter (fn [e] (= "requestId" (first e))))
                           (apply second)
                           String.)
                 record))
               value)))

(defn- send-messages [{:keys [kafka-servers topic headers] :as conf} requestIds]
  (let [value (json/write-str (:value conf))
        {:keys [requestType traceKey] :or {requestType ""}} headers]

    (with-open [producer (K.out/producer {::K/nodes kafka-servers
                                          ::K/serializer.key :string
                                          ::K/serializer.value :string
                                          ::K.out/configuration {"client.id" "my-producer"}})]
      (doseq [n requestIds]
        (K.out/send producer
                    {::K/topic topic
                     ::K/headers {"requestType" (.getBytes requestType)
                                  "requestId" (.getBytes n)
                                  "traceKey" (.getBytes (eval traceKey))}
                     ::K/value value}
                    (fn callback [exception _]
                      (when exception
                        "FAILURE")))))
    requestIds))

(defn- consume-messages [{:keys [kafka-servers response-topic]} requestIds]

  (with-open [consumer (K.in/consumer {::K/nodes              kafka-servers
                                       ::K/deserializer.key   :string
                                       ::K/deserializer.value :string
                                       ::K.in/configuration   {"group.id"           "test-group"}})]
    (K.in/register-for consumer
                       [response-topic])
    (loop [ids requestIds]
      (println (count ids))
      (when (not-empty ids)
        (recur (-> (K.in/poll consumer
                              {::K/timeout [5 :seconds]})
                   (get-fn-requestId?)
                   (remove ids)))))))

(defn- resume [start-time load]
  (let [end-time (Instant/now)
        duration (Duration/between start-time end-time)
        result-str (format "
======================================
load  %d
start %s
end   %s
diff  %d sec
diff  %d min"
                           load start-time end-time
                           (.getSeconds duration)
                           (quot (.getSeconds duration) 60))]

    (println result-str)
    (spit "result.txt" result-str :append true)))

(defn- eval-request-ids [conf load]
  (repeatedly load #(eval (get-in conf [:headers :requestId]))))

(defn- test-kafka [conf load]
  (let [requestIds (eval-request-ids conf load)]
    (send-messages conf requestIds)
    (let [start-time (Instant/now)]
      (println (str start-time " sended " load " messages"))
      (consume-messages conf requestIds)
      (resume start-time load))))

(defn -main [properties & args]
  (let [conf (get-config properties)]
    (if-let [load (first args)]
      (dotimes [n (Integer/parseInt load)] (test-kafka conf (int (Math/pow 10 n))))
      (test-kafka conf (:message-number conf)))))

(comment
  (-main "/home/kromke/Документы/ЕГРН 2/tests-doc/sa-bft-dev.edn" "2")
   )
