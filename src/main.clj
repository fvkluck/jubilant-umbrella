(ns main
  (:require [hyperfiddle.rcf :as rcf]
            [clojure.core.async :refer [go <! >!  <!! chan >!! close! alts!] :as async]))

(comment
  (rcf/enable!))

(defonce logs (atom []))

(defn log [msg]
  (swap! logs conj msg))

(defn show-log []
  @logs)

(defn clear-log []
  (reset! logs []))

(comment
  (clear-log)
  (show-log))

(rcf/tests 
  (def graph
    {:nodes #{:u :v :w :x :y :z}
     :links {[:u :v] 4
             [:u :w] 7
             [:u :x] 9
             [:v :x] 4
             [:v :w] 3
             [:w :x] 2
             [:w :y] 8
             [:x :y] 4
             [:w :z] 3
             [:y :z] 6}}))

(defn distance [link node]
  (let [[[n1 n2] d] link]
    (cond
      (= node n1) [n2 d]
      (= node n2) [n1 d]
      :else nil)))

(rcf/tests
  (distance [[:u :v] 4] :u) := [:v 4]
  (distance [[:u :v] 4] :y) := nil)

(defn worker [{:keys [id]}]
  (atom {:id id}))

(defn make-connection [d]
  {:in (chan)
   :out (chan)
   :distance d})

(defn reverse-connection [{:keys [in out] :as conn}]
  (-> conn
      (assoc :in out)
      (assoc :out in)))

(defn add-connection [w conn]
  (update w :neighbours (fnil conj []) conn))

(defn get-neighbour [w n-id]
  (->> (:neighbours w)
       (filter (fn [conn] (= n-id (:id conn))))
       first))

(defn remove-neighbour [w n-id]
  (update w :neighbours (fn [neighbours] (filter (fn [neighbour] (not= n-id (:id neighbour))) neighbours))))

(defn remove-neighbour-by-in-ch [w ch]
  (update w :neighbours (fn [neighbours] (filter (fn [neighbour] (not= ch (:in neighbour))) neighbours))))

(defn update-neighbour-by-in-ch [w ch f]
  (update w :neighbours #(map (fn [n]
                                (if (= ch (:in n))
                                  (f n)
                                  n)) %)))

(rcf/tests
  (def w (worker {:id :x}))
  (def conn (make-connection 3))
  (swap! w add-connection conn)
  @w
  (count (:neighbours @w)) := 1
  #_(swap! w update-neighbour-by-in-ch (:in conn) (fn [n] (assoc n :foo :bar)))
  (swap! w remove-neighbour-by-in-ch (:in conn))
  (count (:neighbours @w)) := 0)

(defn connect-workers! [w1 w2 d]
  (let [conn (make-connection d)]
    (swap! w1 add-connection conn)
    (swap! w2 add-connection (reverse-connection conn))))

(rcf/tests
  (def wx (atom {:id :x}))
  (def wy (atom {:id :y}))
  (connect-workers! wx wy 3)
  (count (:neighbours @wy)) := 1
  (count (:neighbours @wx)) := 1)

(defmulti handle-message
  "A handle-message should accept a (worker) state and a msg, and return the updated state and a
  list of messages to send to the neighbours of the worker"
  (fn [_state msg ch] (:id msg)))

(defmethod handle-message :greet
  [state {:keys [sender-id] :as msg} ch]
  (log msg)
  (println "Greeting")
  [(-> state
       (update-neighbour-by-in-ch ch (fn [n] (assoc n :id sender-id))))
   [[sender-id {:id :greet-reply
                :sender (:id state)}]]])

(rcf/tests
  (def conn (make-connection 3))
  (def w {:id :x
          :neighbours [conn]})
  (let [[new-state msgs] (handle-message w
                                         {:id :greet
                                          :sender-id :y}
                                         (:in conn))]
    (-> new-state :neighbours first :id) := :y
    (-> (first msgs) first) := :y
    (-> (first msgs) second) := {:id :greet-reply
                                 :sender :x}))

(defmethod handle-message :disconnect
  [state {:keys [sender-id] :as _msg} ch]
  (do
    (println "Closing" ch)
    (close! ch)
    [(remove-neighbour state sender-id) []]))

(rcf/tests
  (def conn (make-connection 3))
  (def w {:id :x
          :neighbours [(assoc conn :id :y)]})
  (let [[new-state msgs] (handle-message w
                                         {:id :disconnect :sender-id :y}
                                         (:in conn))]
    (-> new-state :neighbours count) := 0
    (count msgs) := 0))

(defmethod handle-message :greet-reply
  [state {:keys [_sender-id] :as msg} ch]
  (log msg)
  (println "Greeting reply")
  [state []])

(defmethod handle-message nil
  [state msg ch]
  (log msg)
  (println "Worker" (:id state) "received message:" msg)
  [state []])

(defn listen-neighbours! [worker handler]
  (go
    (loop []
      (when-let [neighbours (:neighbours @worker)]
        (let [in-chans (->> neighbours
                            (map :in))
              [msg ch] (alts! in-chans)]
          (let [[new-state msgs] (handler @worker msg ch)]
            (reset! worker new-state)
            (doseq [[addressee msg] msgs]
              (>! (-> @worker
                      (get-neighbour addressee)
                      :out)
                  msg)))
            (recur))))))

(rcf/tests
  "listen-neighbours! can handle worker without neighbours"
  (def w (worker {:id :x}))
  (listen-neighbours! w handle-message))

(rcf/tests
  (def test-chan (chan 1))
  (defn test-handler [state msg ch]
    (>!! test-chan msg)
    [state []])
  (def in (chan))
  (def out (chan))
  (def w (worker {:id :x}))
  (swap! w add-connection {:in in :out out :distance 4})
  (listen-neighbours! w test-handler)
  (>!! in "Hello")
  (<!! test-chan) := "Hello")

(defn build-network [{:keys [nodes] :as _graph}]
  (into {} (for [n nodes]
             [n (worker {:id n})])))

(defn start-system
  [{:keys [links] :as graph}]
  (let [network (build-network graph)]
    (doseq [[[n1 n2] distance] links]
      (connect-workers! (n1 network) (n2 network) distance)
      (listen-neighbours! (n1 network) handle-message)
      (listen-neighbours! (n2 network) handle-message))
    network))

(rcf/tests
  (def network (start-system graph)))

(rcf/tests
  "greet neighbours"
  (clear-log)
  (def w (:x network))
  (let [w (:x network)]
    (doseq [{:keys [in out d]} (:neighbours @w)]
      (go (>! out {:id :greet
                   :sender-id :x}))))
  (<!! (async/timeout 300))  ; TODO figure out how to naturally wait for the previous statement to finish
  (->> (show-log)
       (map :id)
       frequencies) := {:greet 4 :greet-reply 4})

(comment (let [in (chan)
               out (chan)
               w (-> (worker {:id :x}))]
           (swap! w add-connection {:in in :out out :distance 4})
           (listen-neighbours! w handle-message)
           (>!! in "Hello")
           @w
           (go (<! out))  ; fake :u listening for the reply
           (>!! in {:id :greet
                    :sender-id :u})
           @w
           (>!! in {:id :disconnect
                    :sender-id :x})
           (<!! (async/timeout 100))
           @w))


; old stuff below this line
; ------------------

; example node
{:id :u
 :neighbours {:v 4 :x 9 :w 7}
 :routes {:v [{:path '()
               :distance 4}
              {:path '(:x)
               :distance 13}]}
 :requests {:z [:y :x]} ; :y and :x are looking for a route to :z
 }

(defn shortest-route [self route-target]
  (let [routes (-> self :routes route-target)]
    (apply min-key :distance routes)))

; example messages, not yet how they're currently implemented
{:header {:addressee :y
          :source :x
          :path (list)}
 :body {:message-type :update-route
        :target :u
        :path (list :y :z)
        :distance 10}}

{:header {:source :x
          :addressee :y
          :path (list)}
 :body {:message-type :update-route
        :target :u}}
