

use lahman2017;
use lahman2017raw_pk;
select column_name from (select * from INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME = 'people') as a where a.CONSTRAINT_SCHEMA = 'lahman2017raw_pk' and REFERENCED_TABLE_NAME = 'batting';

Alter table appearances add PRIMARY KEY (`yearID`,`teamID`,`playerID`),
Alter table appearances add CONSTRAINT `apptopeople` FOREIGN KEY (`playerID`) REFERENCES `people` (`playerID`) ON DELETE NO ACTION ON UPDATE NO ACTION,
Alter table appearances add CONSTRAINT `apptoteam` FOREIGN KEY (`yearID`,`teamID`) REFERENCES `Teams` (`yearID`,`teamID`) ON DELETE NO ACTION ON UPDATE NO ACTION;


Alter table batting add PRIMARY KEY (`playerID`,`yearID`,`stint`,`teamID`),
Alter table batting add CONSTRAINT `battingtopeople` FOREIGN KEY (`playerID`) REFERENCES `people` (`playerID`) ON DELETE NO ACTION ON UPDATE NO ACTION,
Alter table batting add CONSTRAINT `battingtoteam` FOREIGN KEY (`yearID`,`teamID`) REFERENCES `Teams` (`yearID`,`teamID`) ON DELETE NO ACTION ON UPDATE NO ACTION;

Alter table Fielding add PRIMARY KEY (`playerID`,`yearID`,`stint`,`teamID`,`POS`,`InnOuts`,`PO`,`E`),
Alter table Fielding add CONSTRAINT `fieldingtopeople` FOREIGN KEY (`playerID`) REFERENCES `people` (`playerID`) ON DELETE NO ACTION ON UPDATE NO ACTION,
Alter table Fielding add CONSTRAINT `fieldingtoteam` FOREIGN KEY (`yearID`,`teamID`) REFERENCES `Teams` (`yearID`,`teamID`) ON DELETE NO ACTION ON UPDATE NO ACTION;

Alter table Managers add PRIMARY KEY (`playerID`,`yearID`,`teamID`,`inseason`),
Alter table Managers add CONSTRAINT `managertopeople` FOREIGN KEY (`playerID`) REFERENCES `people` (`playerID`) ON DELETE NO ACTION ON UPDATE NO ACTION,
Alter table Managers add CONSTRAINT `managertoteam` FOREIGN KEY (`yearID`,`teamID`) REFERENCES `Teams` (`yearID`,`teamID`) ON DELETE NO ACTION ON UPDATE NO ACTION;

Alter table people add PRIMARY KEY (`playerID`,`yearID`,`teamID`,`inseason`);
Alter table Teams add PRIMARY KEY (`yearID`,`teamID`)