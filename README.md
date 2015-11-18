# cs598rk

## Problem statement and motivation  
The abundance of online reviews opened up a new way of designing products. A solid example of this is shown through a company, C&A Marketing Inc., that has a team of workers who read through Amazon reviews, select features that users want, and develop a new product based on the wished-for features. It’s said that “Those reviewers have no idea they just joined the R&D department,” but it’s clear that Amazon reviews can help product designers actively explore features that make a product satisfactory without a formal study [1].  

Our project is to build a company-oriented tool that guides them in design new products through Amazon review analysis. After last release of their products, companies usually need to re-design and improve their product to respond to feedbacks in the market. However, reading all the comments on their products, in addition to their competitors’, is time consuming. Also the tradeoff between the additional manufacturing cost for adding a feature and the product’s popularity might not be clear. There is a vast amount of prior work on summarizing online reviews[2], especially on sentiment analysis of the reviews , and [3] has also explored helping buyers find products with certain features. [4] is most similar to our work, but was limited to the mobile phone and used several regression approaches while our work will explore other methods. More specifically, there has not been a study on exploration of features, ratings, and cost for a wide range of products for designers based on online reviews to our knowledge. The contribution of this work will be in helping companies manage their resource constraints,  such as limitations on price or time, through a visualization. Through our tool, the companies will be able to decide on which features  to add or improve on as well as features to drop, based on the tradeoff between certain features and user ratings and reviews.  

## Methodology  

Use the method in “Opinion observer” [2] to get the feature space of product. With this feature space, and maybe also other factors, like price, to predict the average rating a product will get.  

More details:  
* Perform POS tagging and remove digits  
* Replace actual feature words with [feature] reviews agreed on [4]  
* Generate 3-grams from reviews, which contains the [feature]  
* Distinguish duplicate tags  
* Word stemming  
* Rule mining  
* Extraction of product features  
* Add price, brand prestige into feature space  
* Model relationship between feature space and rating  

## References  

http://www.fastcompany.com/3021229/chaim-pikarski-the-amazon-whisperer  

Liu, Bing, Minqing Hu, and Junsheng Cheng. "Opinion observer: analyzing and comparing opinions on the web." Proceedings of the 14th international conference on World Wide Web. ACM, 2005.  

Scaffidi, Christopher, et al. "Red Opal: product-feature scoring from reviews."Proceedings of the 8th ACM conference on Electronic commerce. ACM, 2007.  

Decker, Reinhold, and Michael Trusov. "Estimating aggregate consumer preferences from online product reviews." International Journal of Research in Marketing 27.4 (2010): 293-307.  

A Python implementation of the Rapid Automatic Keyword Extraction (RAKE) algorithm as described in: Rose, S., Engel, D., Cramer, N., & Cowley, W. (2010). Automatic Keyword Extraction from Individual Documents. In M. W. Berry & J. Kogan (Eds.), Text Mining: Theory and Applications: John Wiley & Sons.


