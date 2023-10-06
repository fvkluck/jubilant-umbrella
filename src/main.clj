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

(comment
  (show-log))

(defn clear-log []
  (reset! logs []))

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

(defn worker [{{:keys [in out distance]} :conn :keys [id message-handler]}]
  (atom {:id id
         :message-handler message-handler})
  )

(defn make-connection [d]
  {:in (chan)
   :out (chan)
   :distance d})

(defn reverse-connection [{:keys [in out] :as conn}]
  (-> conn
      (assoc :in out)
      (assoc :out in)))

(defn add-connection [w conn-id conn]
  (update w :neighbours assoc conn-id conn))

(defn connect-workers! [w1 w2 d]
  (let [conn (make-connection d)]
    (swap! w1 add-connection (:id @w2) conn)
    (swap! w2 add-connection (:id @w1) (reverse-connection conn))))

(rcf/tests
  (def wx (atom {:id :x}))
  (def wy (atom {:id :y}))
  (connect-workers! wx wy 3)
  (nil? (:x (:neighbours @wy))) := false
  (nil? (:y (:neighbours @wx))) := false)

(defn set-message-handler
  "Set the message-handler of worker w to f. f should be a function [w msg] that accepts a worker
  and a msg, and returns the updated worker"
  [w f]
  (assoc w :message-handler f))

(defmulti handle-message
  "A handle-message should accept a (worker) state and a msg, and return the updated state and a
  list of messages to send to the neighbours of the worker"
  (fn [_state msg] (:id msg)))

(defmethod handle-message :greet
  [state msg]
  (log msg)
  (println "Greeting")
  [state []])

(defmethod handle-message nil
  [state msg]
  (log msg)
  (println "Worker" (:id state) "received message:" msg)
  [state []])

(defn listen-neighbours! [worker]
  (go
    (loop []
      (let [in-chans (->> (:neighbours @worker)
                          vals
                          (map :in))
            [msg ch] (alts! in-chans)]  ; TODO no check on no neighbours
        (if (= (:id msg) :disconnect)
          (do
            (println "Closing")
            (close! ch)
            (swap! worker update :neighbours dissoc (:sender-id msg)))
          (do (when-let [handler (:message-handler @worker)]
                (let [[new-state _msgs] (handler @worker msg)]
                  (reset! worker new-state)))
              (recur)))))))

; I think a message handler should be a function that takes the current state and the message, and returns the new state and a list of messages (addressee, content) to send. That way, the message handler can be pure, and the actions are handled in listen-neighbours!

(rcf/tests
  (def test-chan (chan 1))
  (defn test-handler [state msg]
    (>!! test-chan msg)
    [state []])
  (def in (chan))
  (def out (chan))
  (def w (worker {:id :x
                  :message-handler test-handler}))
  (swap! w add-connection :u {:in in :out out :distance 4})
  (listen-neighbours! w)
  (>!! in "Hello")
  (<!! test-chan) := "Hello"

  (>!! in {:id :disconnect
           :sender-id :u})
  (<!! (async/timeout 100))
  (:neighbours @w) := {}
  )

(defn build-network [{:keys [nodes] :as _graph}]
  (into {} (for [n nodes]
             [n (worker {:id n
                         :message-handler handle-message})])))

(defn start-system
  [{:keys [links] :as graph}]
  (let [network (build-network graph)]
    (doseq [[[n1 n2] distance] links]
      (connect-workers! (n1 network) (n2 network) distance)
      (listen-neighbours! (n1 network))
      (listen-neighbours! (n2 network)))
    network))

(rcf/tests
  (def network (start-system graph)))

(comment "greet neighbours"
         (let [w (:x network)]
           (for [[n-id {:keys [in out d]}] (:neighbours @w)]
             (go (>! out :greet)))))

(comment (let [in (chan)
               out (chan)
               w (-> (worker {:id :x
                              :message-handler handle-message}))]
           (swap! w add-connection :u {:in in :out out :distance 4})
           (listen-neighbours! w)
           (>!! in "Hello")
           @w
           (>!! in {:id :greet})
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
