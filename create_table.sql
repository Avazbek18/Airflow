create pageview_counts (
pagename varchar(50) not null, 
pageviewcount int not null, 
datetime timestamp not null, 
constraint unique_page_date unique (pagename, datetime)
);