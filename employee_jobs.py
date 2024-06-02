from mrjob.job import MRJob
from mrjob.step import MRStep

class EmployeeStats(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_collect,
                   reducer=self.reducer_collect),
            MRStep(reducer=self.reducer_final)
        ]

    def mapper_collect(self, _, line):
        fields = line.split(',')
        try:
            idemp = fields[0]
            sececon = fields[1]
            salary = float(fields[2])
            yield idemp, (sececon, salary)
        except:
            pass

    def reducer_collect(self, key, values):
        sececons = set()
        total_salary = 0
        salary_count = 0
        sector_salary = {}

        for value in values:
            sececon, salary = value
            sececons.add(sececon)
            total_salary += salary
            salary_count += 1

            if sececon in sector_salary:
                sector_salary[sececon][0] += salary
                sector_salary[sececon][1] += 1
            else:
                sector_salary[sececon] = [salary, 1]

        yield key, (total_salary, salary_count, len(sececons), sector_salary)

    def reducer_final(self, key, values):
        total_salary = 0
        salary_count = 0
        num_sectors = 0
        sector_salary = {}

        for value in values:
            total_salary += value[0]
            salary_count += value[1]
            num_sectors = value[2]

            for sececon, (salary_sum, count) in value[3].items():
                if sececon in sector_salary:
                    sector_salary[sececon][0] += salary_sum
                    sector_salary[sececon][1] += count
                else:
                    sector_salary[sececon] = [salary_sum, count]

        avg_salary_employee = total_salary / salary_count if salary_count else 0

        sector_avg_salaries = {sececon: salary_sum / count for sececon, (salary_sum, count) in sector_salary.items()}

        yield key, {
            'avg_salary_employee': avg_salary_employee,
            'num_sectors': num_sectors,
            'sector_avg_salaries': sector_avg_salaries
        }

if __name__ == '__main__':
    EmployeeStats.run()