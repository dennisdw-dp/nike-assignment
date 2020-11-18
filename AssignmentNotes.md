Thank you for taking a look at my solution to this assignment.

The choice for Scala over the other language options was quite simple. I have used both Scala and Python for Spark programming
before, and find both very useful options. However, I wanted the job specification to be given as json, and had done this
very recently for a Scala/Spark project. Doing the same thing in Python would have taken extra time without any real benefit.

I chose to have the input specified in json rather than, for example, as command-line parameters mainly because this
would allow these jobs to be version controlled. I have recently done something very similar for a different project,
so it was quite simple to do it again here with that example in mind.

When I started I wanted to offer options for I/O to AWS as well as local files, but ended up dropping that due to time constraints.
It should be pretty simple to add by creating new subclasses of InputParameters and OutputParameters, which would contain
the information necessary to use the S3 API (bucket and key, as well as authentication data).

There a few points where I feel significant improvements could be made in addition to AWS support.

Most importantly, the calendar data as given contains the week number per season, but no indication of which season
it belongs to. It would be possible to infer that information from the order in which the calendar data points appear.
(i.e. the first occurrence of week 1 would be week 1 of Q1, whereas the second occurence would be week 1 of Q2)
However, that would assume the order of the data is guaranteed, which is not specified.
For now, there will only be data for weeks 1 through 13.

Second, the input data should be validated more thoroughly before it is used. I currently assume that it will be in the correct
format (i.e. the format of the files in the assignment), but better checks ought to be in place to ensure this.

The assignment did not mention the output format for data aggregated by year. If that format were given, it should be
implemented so the output matches the expectations in that case as well.

Finally, the `assembleFinalWeeklyData` and `assembleFinalYearlyData` are not coverd by unit tests due to time constraints.
While they work when the program is executed normally, of course proper tests are still required.


To answer the questions in the assignment text:

>How long did you spend on the coding test? What would you add to your solution if you had more time? If you didn't spend much time on the coding test then use this as an opportunity to explain what you would add.

I took around 8 hours all in all. The points I would improve on given more time are listed above.

>What was the most useful feature that was added to the latest version of your chosen language? Please include a snippet of code that shows how you've used it.

Despite Scala 2.13 being out for a while now, I mostly still have to use version 2.12 due to other libraries not being updated yet.
However, at the time of writing version 2.12.13 should be coming out any time now. While this is a minor update, it contains a bugfix
backported from Scala 2.13 which I really need in a different project. Perhaps not the most flashy feature, but definitely one I can put to good use.

Specifically, I refer to this fix: https://github.com/scala/scala/pull/9230. The bug it fixes prevents me from accessing a package-private
object through reflection with ScriptEngineManager. It's a bit difficult to provide a single snippet, but I'd be happy to explain it in person.

>How would you track down a performance issue in production? Have you ever had to do this the above task with the terabyte of data?

I have only done this for a PySpark project before, and not nearly with a terabyte worth of data. However, the process was
essentially to look at the Spark logs in the Spark UI and see which tasks were taking up most of the time and if we could spot
any data flow issues. While it's a little difficult to relate those log entries directly to Python code due to the way the code
is executed on the JVM, this process did help us find cases where we were not caching data were we should have been,
leading to duplicate work being done.