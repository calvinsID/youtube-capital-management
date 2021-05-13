The project consists of 3 azure functions: pullyoutubecomments, pullyoutubecommentslivestream and purchasestonks

  - pullyoutubecomments: get all youtube comments from the video (https://www.youtube.com/watch?v=5f7Zw3gAZo0&t=1s) and put stock transaction messages in azure queue

  - pullyoutubecommentslivestream: get all youtube comments from the livestream (https://www.youtube.com/watch?v=ZewaUefc-gM) and put stock transaction messages in azure queue

  - purchasestonks: get stock transaction messages from azure queue, and execute a trade using Alpaca trading API


To publish:
  - func azure functionapp publish pull-youtube-comments --build remote