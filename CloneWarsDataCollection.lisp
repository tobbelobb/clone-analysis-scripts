;;; Script to generate tree structure of RepRap owners out of data behind
;;; http://maytheclonebewithyou.com/

;; API is simply
;; http://maytheclonebewithyou.com/index.php/detail?n=1
;; where n goes from 1 to 562 or something

;; To make http requests, use for example drakma
;; (ql:quickload :drakma)
;; (drakma:http-request "http://maytheclonebewithyou.com/index.php/detail?n=1")
;; This returns 8 things, of which the first is a JSON data structure with the stuff we want in it

;; The stuff we want:
;;   "printername"  : Printer's name
;;   "madre"        : Printer's mother's name
;;   "username"     : Printer's owner
;;   "printernumber": Well, maybe printer number (id) is useful to

;; drakma uses cl-ppcre, so we might as well use regex instead of loading some
;;   additional JSON parser thing. Strings are quite short anyways.
(defun field-regex (s)
  "Returns a regex to match a field with the name that is is the string s"
  `(:sequence ,s                                                           ; The field name
              #\"                                                          ; End of the key name string
              (:greedy-repetition 0 nil :whitespace-char-class)            ; Any amount of white space
              #\:                                                          ; Key-val separating colon
              #\"                                                          ; Start val string
              (:register                                                   ; Catch the val
                (:non-greedy-repetition 1 nil :everything)) ; The val
              #\"                                                          ; End of val string
              (:greedy-repetition 0 nil :whitespace-char-class)            ; Any amount of white space
              (:alternation #\, #\})))                                     ; Key-val end with commas or end brackets


(defparameter *printernumber-regex* (field-regex "printernumber"))
(defparameter *madre-regex*         (field-regex "madre"))
(defparameter *printername-regex*   (field-regex "printername"))
(defparameter *username-regex*      (field-regex "username"))

;; Let's match all four fields in one go, and pay the price of depending on their order
(defparameter *all-four-regex* `(:sequence ,@(rest *printernumber-regex*)            ; Splice previous sequences into new longer sequence
                                             (:greedy-repetition 0 nil :everything)
                                           ,@(rest *madre-regex*)
                                             (:greedy-repetition 0 nil :everything)
                                           ,@(rest *printername-regex*)
                                             (:greedy-repetition 0 nil :everything)
                                           ,@(rest *username-regex*)))

;; This has the advantage that we can bind all fields to variable in one go

(defun clone-data (n &optional (out t))
  "Interface with maytheclonebewithyou API to extract data from clone number n"
  (cl-ppcre:register-groups-bind (id madre pname uname)
                                   (*all-four-regex*
                                     (drakma:http-request
                                       (format nil "http://maytheclonebewithyou.com/index.php/detail?n=~a" n)))
                                   (format out "~a, ~a, ~a, ~a~%" id madre pname uname)))

; used filename "/home/torbjorn/D3D/Thesis/data/clones.data"
(defun collect-all-clone-data (filename)
  (with-open-file (filestream filename :direction :output :if-exists :append :if-does-not-exist :create)
    (do ((n 1 (incf n)))
      ((= n 552)) ; There were 552 printers in database at the time of writing this code
      (cl-ppcre:register-groups-bind (id madre pname uname)
                                     (*all-four-regex*
                                       (drakma:http-request
                                         (format nil "http://maytheclonebewithyou.com/index.php/detail?n=~a" n)))
                                     (format filestream "~a, ~a, ~a, ~a~%" id madre pname uname)))))
