# Quora Answer Prediction
Quora uses a combination of machine learning algorithms and moderation to ensure high-quality content on the site. High question and answer quality has helped Quora distinguish itself from other Q&A sites on the web.

As they get many questions every day, a challenge we have is to figure out good, interesting and meaningful questions from the bad. What questions are valid ones that can be answered? What questions attract reputable answers that then get upvoted? 

This implementation tells which questions will likely get answers quickly, so that the questions can be shown in real-time to the users.

For this task, given Quora question text and topic data, predict whether a question gets an upvoted answer within 1 day.

### Input Format
The first line contains N. N questions follow, each being a valid json object. The following fields of raw data are given in json.
* question_key (string): Unique identifier for the question.
* question_text (string): Text of the question.
* context_topic (object): The primary topic of a question, if present. Null otherwise. The topic object will contain a name (string) and followers (integer) count.
* topics (array of objects): All topics on a question, including the primary topic. Each topic object will contain a name (string) and followers (integer) count.
* anonymous (boolean): Whether the question was anonymous.
* __ans__ (boolean): Whether the question got an up-voted answer within 1 day.

This is immediately followed by an integer T.

T questions follow, each being a valid json object.

The json contains all but one field __ans__.

### Output Format
T rows of JSON encoded fields, with the question_key key containing the unique identifier given in the test data, and the predicted value keyed by __ans__.

### Constraints
question_key is of ascii format.

question_text, name in topics and context_topic is of UTF-8 format.

0 <= followers <= 106

9000 <= N <= 45000

1000 <= T <= 5000
