(ns event-data-common.backoff-tests
  (:require [clojure.test :refer :all]
            [event-data-common.backoff :as backoff]
            [clojure.core.async :as async]))

; Because try-backoff executes asynchronously, we need to pause the test until the right behaviour has taken place.
; Monitors are implemented as promises which are delivered by callbacks when the expected target has been reached.

(deftest ^:unit immediate-success
  (let [f-counter (atom 0)
        error-counter (atom 0)
        terminate-counter (atom 0)
        finally-counter (atom 0)
        
        finally-called (promise)
       
        f (fn [] (swap! f-counter inc))

        error-f (fn [ex] (swap! error-counter inc))
        terminate-f (fn [] (swap! terminate-counter inc))
        finally-f (fn [] (swap! finally-counter inc)
                         (deliver finally-called 1))]

    (testing "Callbacks should be called the correct number of times on immediate success."
      ; Run a function that works first time.
      ; Zero timer because we don't care about timing, but do want fast tests.
      (backoff/try-backoff f 0 5 error-f terminate-f finally-f)

      ; Wait for finally to be called.
      ; @finally-called

      (is (= 1 @f-counter) "f should have been called once if it succeeded.")
      (is (= 0 @error-counter) "error should not be called on success")
      (is (= 0 @terminate-counter) "terminate should not be called on success")
      (is (= 1 @finally-counter) "finally should always be called once"))))

(deftest ^:unit initial-failure
  (let [f-counter (atom 0)
        error-counter (atom 0)
        terminate-counter (atom 0)
        finally-counter (atom 0)
        
        finally-called (promise)
       
        f (fn []
           ; Exceptions 3 times, then OK.
           (if (<= (swap! f-counter inc) 3)
             (throw (new Exception "No running in pool area!"))
              :everthing-is-fine))

        error-f (fn [ex] (swap! error-counter inc))
        terminate-f (fn [] (swap! terminate-counter inc))
        finally-f (fn [] (swap! finally-counter inc)
                         (deliver finally-called 1))]

    (testing "Callbacks should be called the correct number of times on immediate failure but eventual success."
      ; Run a function that works first time.
      ; Zero timer because we don't care about timing, but do want fast tests.
      (backoff/try-backoff f 0 5 error-f terminate-f finally-f)

      ; Wait for finally to be called.
      @finally-called

      (is (= 4 @f-counter) "f should have been called every time until it succeeds")
      (is (= 3 @error-counter) "error should have been called every error")
      (is (= 0 @terminate-counter) "terminate should not be called on eventual success")
      (is (= 1 @finally-counter) "finally should always be called once"))))

(deftest ^:unit ultimate-failure
  (let [f-counter (atom 0)
        error-counter (atom 0)
        terminate-counter (atom 0)
        finally-counter (atom 0)
        
        finally-called (promise)
       
        f (fn []
            (swap! f-counter inc)
            (throw (new Exception "It'll never work.")))

        error-f (fn [ex] (swap! error-counter inc))
        terminate-f (fn [] (swap! terminate-counter inc))
        finally-f (fn [] (swap! finally-counter inc)
                         (deliver finally-called 1))]

    (testing "Callbacks should be called the correct number of times on continued failure."
      ; Run a function that works first time.
      ; Zero timer because we don't care about timing, but do want fast tests.
      (backoff/try-backoff f 0 5 error-f terminate-f finally-f)

      ; Wait for finally to be called.
      @finally-called

      (is (= 5 @f-counter) "f should have been called every time until it succeeds")
      (is (= 5 @error-counter) "error should have been called every error")
      (is (= 1 @terminate-counter) "terminate should not be called on eventual success")
      (is (= 1 @finally-counter) "finally should always be called once"))))

(deftest ^:unit can-run-concurrently
  (let [f-counter-1 (atom 0)
        error-counter-1 (atom 0)
        terminate-counter-1 (atom 0)
        finally-counter-1 (atom 0)

        f-counter-2 (atom 0)
        error-counter-2 (atom 0)
        terminate-counter-2 (atom 0)
        finally-counter-2 (atom 0)
        
        finally-called-1 (promise)
        finally-called-2 (promise)

        snoop-f-counter-1 (atom nil)
       
        ; First one errors three times then succeeds.
        f-1 (fn []
              (condp = (swap! f-counter-1 inc)
                0 (throw (new Exception ""))
                1 (throw (new Exception ""))
                3 (throw (new Exception ""))
                4 :ok))

        ; Second one runs at the same time, but on the third attempt looks at the counter for
        ; the first, to demonstrate that it is running at the same time.
        f-2 (fn []
             (condp = (swap! f-counter-2 inc)
              0 (throw (new Exception ""))
              1 (throw (new Exception ""))
              ; On the third attempt, save a copy of the counter for the first function.
              2 (do (reset! snoop-f-counter-1 @f-counter-1)
                    (throw (new Exception "")))
              3 (throw (new Exception ""))
              4 :ok))

        error-f-1 (fn [ex] (swap! error-counter-1 inc))
        terminate-f-1 (fn [] (swap! terminate-counter-1 inc))
        finally-f-1 (fn [] (swap! finally-counter-1 inc)
                         (deliver finally-called-1 1))


        error-f-2 (fn [ex] (swap! error-counter-2 inc))
        terminate-f-2 (fn [] (swap! terminate-counter-2 inc))
        finally-f-2 (fn [] (swap! finally-counter-2 inc)
                           (deliver finally-called-2 1))


        ]

    (testing "Functions should run concurrently."
      ; Start the second one after the first.
      ; 100 milliseconds is enought to be confident for tests.
      (backoff/try-backoff f-1 100 5 error-f-1 terminate-f-1 finally-f-1)
      (backoff/try-backoff f-2 100 5 error-f-2 terminate-f-2 finally-f-2)

      ; Only wait for the first one to finish.
      @finally-called-1

      (is (= 1 @finally-counter-1) "finally have been called for first")
      (is (> @snoop-f-counter-1 0) "first counter should have been incremented beyond zero by the time the second failed on a subsequent attempt")))) 
