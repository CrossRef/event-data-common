(ns event-data-common.storage.s3
  "Storage interface for AWS S3."
  (:require [event-data-common.storage.store :refer [Store]]
            [event-data-common.storage.store :as store]
            [clojure.tools.logging :as l]
            [config.core :refer [env]])
  (:import [com.amazonaws.services.s3 AmazonS3 AmazonS3Client]
           [com.amazonaws.auth BasicAWSCredentials]
           [com.amazonaws.services.s3.model GetObjectRequest PutObjectRequest ObjectMetadata S3Object ObjectListing S3ObjectSummary ListObjectsRequest]
           [com.amazonaws AmazonServiceException AmazonClientException]
           [com.amazonaws.regions Regions Region]
           [org.apache.commons.io IOUtils]
           [java.io InputStream]))

; Default retry policy of 3 to cope with failure.
; http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/ClientConfiguration.html#DEFAULT_RETRY_POLICY
(defn get-aws-client
  [s3-key s3-secret s3-region-name]
  (let [client (new AmazonS3Client (new BasicAWSCredentials s3-key s3-secret))]
    (.setRegion client (Region/getRegion (Regions/fromName s3-region-name)))
    client))

(defn upload-string
  "Upload a stream. Exception on failure."
  [client bucket-name k v content-type]
  (let [bytes (.getBytes v)
        metadata (new ObjectMetadata)
        _ (.setContentType metadata content-type)
        _ (.setContentLength metadata (alength bytes))
        request (new PutObjectRequest bucket-name k (new java.io.ByteArrayInputStream bytes) metadata)]
    (.putObject client request)))

(defn download-string
  "Download a stream. Exception on failure."
  [client bucket-name k]
  (when (.doesObjectExist client bucket-name k)
    (let [^S3Object object (.getObject client bucket-name k)
          ^InputStream input (.getObjectContent object)
          result (IOUtils/toString input "UTF-8")]
      (.close input)
      result)))

(defn get-keys
  "Return a seq of String keys for an ObjectListing"
  [^ObjectListing listing]
  (map #(.getKey ^S3ObjectSummary %) (.getObjectSummaries listing)))

(defn list-objects
  "List the next page of objects, or the first page if prev-listing is nil.
   If delimiter is supplied, list common key strings up to and including delimiter."
  [client bucket-name prefix delimiter prev-listing]
  (if-not prev-listing
    ; Start at first page.
  
    (let [^ListObjectsRequest request (new ListObjectsRequest bucket-name prefix nil delimiter nil)
          ^ObjectListing this-listing (.listObjects client request)]
      (if-not (.isTruncated this-listing)
        ; This is the last page return keys, or parts of keys if there was a delimiter.
        (if delimiter
          (.getCommonPrefixes this-listing)
          (get-keys this-listing))

        ; This is not the last page.
        ; Recurse with the listing as the next-page token.
        (lazy-cat (get-keys this-listing) (list-objects client bucket-name prefix this-listing delimiter))))

    ; Start at subsequent page.
    (let [^ObjectListing this-listing (.listNextBatchOfObjects client prev-listing)]
      (if-not (.isTruncated this-listing)
        ; This is the last page return keys, or parts of keys if there was a delimiter.
        (if delimiter
          (.getCommonPrefixes this-listing)
          (get-keys this-listing))

        ; This is not the last page.
        ; Recurse with the listing as the next-page token.
        (lazy-cat (get-keys this-listing) (list-objects client bucket-name prefix this-listing delimiter))))))

(defprotocol S3
  "AWS S3-specific interface."
  (list-with-delimiter [this prefix delimiter] "List keys with prefix, up to and including delimiter. See S3 API definition for more info."))

(defrecord S3Connection
  [^AmazonS3Client client s3-bucket-name]
  
  Store
  (get-string [this k]
    (l/debug "Store get" k)
    (download-string client s3-bucket-name k))

  (set-string [this k v]
    (l/debug "Store set" k)
    (upload-string client s3-bucket-name k v "application/json"))

  (keys-matching-prefix [this prefix]
    (l/debug "Store scan prefix" prefix)
    (list-objects client s3-bucket-name prefix nil nil))

  (delete [this k]
    (l/debug "Delete key" k)
    (.deleteObject client s3-bucket-name k))

  S3
  (list-with-delimiter [this prefix delimiter]
    (list-objects client s3-bucket-name prefix delimiter nil)))

(defn build
  "Build a S3Connection object."
  [s3-key s3-secret s3-region-name s3-bucket-name]
  (let [client (get-aws-client s3-key s3-secret s3-region-name)]
    (S3Connection. client s3-bucket-name)))

