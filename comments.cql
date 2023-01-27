CREATE KEYSPACE IF NOT EXISTS comments
  WITH REPLICATION = { 
   'class' : 'SimpleStrategy',
   'replication_factor' : 1 
  };

USE comments;

CREATE TABLE IF NOT EXISTS comments_by_video
(
  videoid int,
  commentid timeuuid,
  userid uuid,
  comment text,
  PRIMARY KEY (videoid, commentid)
)
WITH CLUSTERING ORDER BY (commentid DESC);

INSERT INTO comments_by_video (videoid, userid, commentid, comment)
   VALUES (1, d0f60aa8-54a9-4840-b70c-fe562b68842b, now(), 'Worst. Video. Ever.');

INSERT INTO comments_by_video (videoid, userid, commentid, comment)
   VALUES (1, 522b1fe2-2e36-4cef-a667-cd4237d08b89, now(), 'It is amazing');

INSERT INTO comments_by_video (videoid, userid, commentid, comment)
   VALUES (1, 49f64d40-7d89-4890-b910-dbf923563a33, now(), 'Cool');


INSERT INTO comments_by_video (videoid, userid, commentid, comment)
   VALUES (2, d0f60aa8-54a9-4840-b70c-fe562b68842b, now(), 'Best. Video. Ever.');

INSERT INTO comments_by_video (videoid, userid, commentid, comment)
   VALUES (2, 522b1fe2-2e36-4cef-a667-cd4237d08b89, now(), 'It is crap');

INSERT INTO comments_by_video (videoid, userid, commentid, comment)
   VALUES (2, 49f64d40-7d89-4890-b910-dbf923563a33, now(), 'Great');



