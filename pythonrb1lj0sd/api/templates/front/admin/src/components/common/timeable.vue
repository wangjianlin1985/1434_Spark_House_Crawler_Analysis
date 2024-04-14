<template>
	<div>
		<div class="panel">
			<el-table :data="timetable" :span-method="objectSpanMethod" border
				:header-cell-style="{background:'#d9e5fd', color:'black', fontWeight: 1000}"
				:cell-style="tableCellStyle">
				<el-table-column prop="sjd" label="时间段" width="80" align="center">
				</el-table-column>
				<el-table-column prop="jc" label="节次" width="120" align="center">
					<template slot-scope="scope">
						<div v-html="scope.row.jc"></div>
					</template>
				</el-table-column>
				<el-table-column prop="mon" label="星期一" align="center">
					<template slot-scope="scope">
						<h4>{{ scope.row.mon.title }}</h4>
						<div v-html="scope.row.mon.content"></div>
					</template>
				</el-table-column>
				<el-table-column prop="tue" label="星期二" align="center">
					<template slot-scope="scope">
						<h4>{{ scope.row.tue.title }}</h4>
						<div v-html="scope.row.tue.content"></div>
					</template>
				</el-table-column>
				<el-table-column prop="wed" label="星期三" align="center">
					<template slot-scope="scope">
						<h4>{{ scope.row.wed.title }}</h4>
						<div v-html="scope.row.wed.content"></div>
					</template>
				</el-table-column>
				<el-table-column prop="thu" label="星期四" align="center">
					<template slot-scope="scope">
						<h4>{{ scope.row.thu.title }}</h4>
						<div v-html="scope.row.thu.content"></div>
					</template>
				</el-table-column>
				<el-table-column prop="fri" label="星期五" align="center">
					<template slot-scope="scope">
						<h4>{{ scope.row.fri.title }}</h4>
						<div v-html="scope.row.fri.content"></div>
					</template>
				</el-table-column>
				<el-table-column prop="sat" label="星期六" align="center">
					<template slot-scope="scope">
						<h4>{{ scope.row.sat.title }}</h4>
						<div v-html="scope.row.sat.content"></div>
					</template>
				</el-table-column>
				<el-table-column prop="sun" label="星期日" align="center">
					<template slot-scope="scope">
						<h4>{{ scope.row.sun.title }}</h4>
						<div v-html="scope.row.sun.content"></div>
					</template>
				</el-table-column>
			</el-table>
		</div>
	</div>
</template>
<script>
	export default {
		props: {
			// 下午节次数
			afternoonLength: {
				type: [String, Number],
				default: 4
			},
			morningLength: {
				type: [String, Number],
				default: 4
			},
			// 总节次
			length: {
				type: [String, Number],
				default: 12
			},
			// 课表数据
			events: {
				type: Array,
				default: () => []
			},
			time: {
				type: Array,
				default: () => []
			}
		},
		data() {
			return {
				// 课程表数据
				timetable: [],
				hoverOrderArr: [],
				weeks: ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'],
				allPalette: ['#f05261', '#48a8e4', '#ffd061', '#52db9a', '#70d3e6', '#52db9a', '#3f51b5',
					'#f3d147', '#4adbc3', '#673ab7', '#f3db49', '#76bfcd', '#b495e1', '#ff9800', '#8bc34a'
				]
			}
		},
		mounted() {
			this.makeTimetable()
		},
		watch: {
			events: {
				handler(newVal, oldVal) {
					this.mergeData()
				},
				deep: true
			},
			time: {
				handler(newVal, oldVal) {
					this.makeTimetable()
				},
				deep: true
			},
		},
		created() {},
		methods: {
			getRandomInt(min, max) {
			  min = Math.ceil(min);
			  max = Math.floor(max);
			  return Math.floor(Math.random() * (max - min + 1)) + min;
			},
			// 单元格添加背景色
			tableCellStyle(row) {
				if (row.column.property != 'jc' && row.column.property != 'sjd' && row.row[row.column.property].title !==
					undefined) {
					return `background-color:${this.allPalette[this.getRandomInt(0,this.allPalette.length - 1)]};color: #fff; border-radius:10px`
				}
			},
			// 构造课程表完整数据
			makeTimetable() {
				this.timetable = []
				for (let i = 0; i < this.time.length; i++) {
					let one = {
						sjd: this.time[i].sjd,
						jc: Number(this.time[i].sectionnum) + '<br><div style="font-size: 12px">(' + this.time[i]
							.starttime + '-' + this.time[i].endtime + ')</div>',
						jc1: i + 1,
						mon: {},
						tue: {},
						wed: {},
						thu: {},
						fri: {},
						sat: {},
						sun: {}
					}
					this.timetable.push(one)
				}
				this.mergeData()
				this.$forceUpdate()
			},
			dateState(date = new Date()) {
				// 获取当前小时
				let hours = date.getHours()
				if (hours <= 12) {
					return '上午'
				} else if (hours > 12 && hours <= 18) {
					return '下午'
				} else {
					return '晚上'
				}
			},
			mergeData() {
				// 合并数据
				if (this.events.length > 0 && this.timetable.length > 0) {
					for (let i = 0; i < this.events.length; i++) {
						// 获取星期几
						let week = this.weeks[this.events[i].week - 1]
						this.timetable[this.events[i].start - 1][week] = this.events[i]
					}
				}
				console.log(this.timetable)
			},
			objectSpanMethod({
				row,
				column,
				rowIndex,
				columnIndex
			}) {
				const weeks = this.weeks[columnIndex - 2];
				if (columnIndex === 0) {
					if (rowIndex < this.morningLength) {
						if (rowIndex === 0) {
							return {
								rowspan: this.morningLength,
								colspan: 1
							}
						} else {
							return {
								rowspan: 0,
								colspan: 0
							}
						}
					} else if (rowIndex > this.morningLength - 1 && rowIndex < (this.morningLength + this
						.afternoonLength)) {
						if (rowIndex === this.morningLength) {
							return {
								rowspan: this.afternoonLength,
								colspan: 1
							}
						} else {
							return {
								rowspan: 0,
								colspan: 0
							}
						}
					} else {
						if (rowIndex === (this.morningLength + this.afternoonLength)) {
							return {
								rowspan: this.length - this.morningLength - this.afternoonLength,
								colspan: 1
							}
						} else {
							return {
								rowspan: 0,
								colspan: 0
							}
						}
					}
				}
				if (columnIndex >= 2) {
					if (row[weeks].title !== undefined &&row.jc1 === row[weeks].start) {
						return {
							rowspan: row[weeks].end - row[weeks].start + 1,
							colspan: 1
						}
					} else if(this.timetable.some(r => r[weeks].start <= row.jc1 && r[weeks].end >= row.jc1)) {
						return {
							rowspan: 0,
							colspan: 1
						}
					}
				}
				// if (columnIndex === 3) {
				// 	console.log(row)
				// 	if (row.tue.title !== undefined) {
				// 		return {
				// 			rowspan: row.tue.end - row.tue.start + 1,
				// 			colspan: 1
				// 		}
				// 	} else {
				// 		return {
				// 			rowspan: 1,
				// 			colspan: 1
				// 		}
				// 	}
				// }
				// if (columnIndex === 4) {
				// 	if (row.wed.title !== undefined) {
				// 		return {
				// 			rowspan: row.wed.end - row.wed.start + 1,
				// 			colspan: 1
				// 		}
				// 	} else {
				// 		return {
				// 			rowspan: 1,
				// 			colspan: 1
				// 		}
				// 	}
				// }
				// if (columnIndex === 5) {
				// 	if (row.thu.title !== undefined) {
				// 		return {
				// 			rowspan: row.thu.end - row.thu.start + 1,
				// 			colspan: 1
				// 		}
				// 	} else {
				// 		return {
				// 			rowspan: 1,
				// 			colspan: 1
				// 		}
				// 	}
				// }
				// if (columnIndex === 6) {
				// 	if (row.fri.title !== undefined) {
				// 		return {
				// 			rowspan: row.fri.end - row.fri.start + 1,
				// 			colspan: 1
				// 		}
				// 	} else {
				// 		return {
				// 			rowspan: 1,
				// 			colspan: 1
				// 		}
				// 	}
				// }
				// if (columnIndex === 7) {
				// 	if (row.sat.title !== undefined) {
				// 		return {
				// 			rowspan: row.sat.end - row.sat.start + 1,
				// 			colspan: 1
				// 		}
				// 	} else {
				// 		return {
				// 			rowspan: 1,
				// 			colspan: 1
				// 		}
				// 	}
				// }
				// if (columnIndex === 8) {
				// 	if (row.sun.title !== undefined) {
				// 		return {
				// 			rowspan: row.sun.end - row.sun.start + 1,
				// 			colspan: 1
				// 		}
				// 	} else {
				// 		return {
				// 			rowspan: 1,
				// 			colspan: 1
				// 		}
				// 	}
				// }
			}
		}
	}
</script>