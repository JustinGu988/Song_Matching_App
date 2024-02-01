package SongMatch

case class inputData(unique_input_record_identifier: String,
                     input_title: String,
                     input_writers: String,
                     input_performers: String)

case class lookUpData(lookup_key: String,
                      lookup_song_code: String, //database_song_code
                      lookup_writers: String, //database_abb05v_writers
                      usage_count: String)

case class songCodeData(database_song_code: String,
                        database_song_title: String,
                        database_song_writers: String)

case class matchedData(unique_input_record_identifier_matched400: String,
                       input_title_matched400: String,
                       lookup_key_matched400: String,
                       input_writers_matched400: String,
                       input_performers_matched400: String,
                       database_song_code_matched400: String,
                       database_song_title_matched400: String,
                       database_song_writers_matched400: String)

case class inputDataProcessed(unique_input_record_identifier: String,
                              input_title: String,
                              input_writers: String,
                              input_performers: String,
                              my_lookup_key: String)

case class output7Col(unique_input_record_identifier: String,
                      input_title: String,
                      lookup_key: String,
                      lookup_key_given: String,
                      database_song_code: String,
                      database_song_title: String,
                      database_song_writers: String)

case class testTitleData(input_title_matched400: String)

case class testExpectedTitleData(input_title_matched400: String,
                                 my_lookup_key: String)