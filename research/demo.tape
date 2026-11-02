# vhs research/demo.tape -> research/demo.gif (connects to the live public feeds)
Output research/demo.gif
Set Shell "bash"
Set FontSize 14
Set Width 1100
Set Height 620
Set Framerate 10
Hide
Type "bin/marketmux -symbols BTC-USD,ETH-USD,SOL-USD -duration 40s -listen 127.0.0.1:8080 -grpc '' -log error > /dev/null 2>&1 & sleep 6; clear"
Enter
Sleep 7s
Show
Type "bin/mmwatch -url 'ws://127.0.0.1:8080/ws?policy=conflate' -duration 18s"
Enter
Sleep 19s
