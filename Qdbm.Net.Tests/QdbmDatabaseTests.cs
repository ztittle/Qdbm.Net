using System;
using System.Collections.Generic;
using System.IO;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Qdbm.Net.Tests
{
    [TestClass]
    public class QdbmDatabaseTests
    {
        private QdbmDatabase _qdbm;
        private MemoryStream _ms;
        
        [TestInitialize]
        public void Setup()
        {
            _ms = new MemoryStream();
            _qdbm = new QdbmDatabase(_ms, 100);
        }
        
        [TestMethod]
        public void PutTest()
        {
            #region TestKeys
            var keys = new[]
            {
                3355440,
                1257163,
                6606064,
                3472651,
                3333598,
                14795062,
                9095027,
                12901197,
                12859379,
                7080927,
                9342832,
                9172220,
                673511,
                9832211,
                390468,
                2141623,
                768312,
                7263769,
                3487677,
                3333802,
                3340294,
                3317389,
                9262108,
                6550272,
                9320991,
                9546247,
                3331752,
                5874866,
                8101445,
                9141227,
                9356175,
                7250409,
                398768,
                7221939,
                7340252,
                7217594,
                421137,
                11728648,
                15077397,
                9579907,
                3314661,
                456931,
                4507979,
                11379799,
                3335464,
                3493405,
                4128107,
                14760396,
                5123274,
                7316996,
                7263399,
                9839631,
                1173720,
                3317291,
                2022739,
                14741634,
                1000290,
                9581729,
                7337259,
                9353235,
                1857106,
                4219631,
                5212922,
                7240003,
                4310366,
                2796532,
                2651230,
                3629332,
                3669944,
                5439991,
                9314259,
                1877563,
                4760801,
                12856211,
                4505325,
                4789648,
                786554,
                659294,
                9126066,
                2579590,
                2780312,
                7238436,
                683817,
                11598045,
                4976626,
                8100390,
                3493381,
                3344429,
                9390209,
                9138401,
                9132501,
                7335197,
                1996957,
                7334058,
                3341734,
                12894430,
                4671044,
                5865667,
                659624,
                1183288,
                3486394,
                1754762,
                3347676,
                3324517,
                7322656,
                12232975,
                5924874,
                12906275,
                709570,
                3374721,
                12903838,
                10016072,
                4721117,
                7318459,
                9528569,
                12858084,
                4940741,
                11673490,
                3314539,
                9858913,
                3344275,
                9580543,
                3513769,
                3474980,
                12905392,
                458695,
                4669335,
                3484343,
                4777167,
                7265721,
                4761625,
                9107001,
                4411694,
                5098360,
                2057844,
                3712025,
                6844634,
                7217181,
                3483175,
                9233788,
                7339246,
                9890076,
                14005999,
                2769689,
                7343012,
                3343642,
                11774863,
                1197024,
                401669,
                3945529,
                7332326,
                3337216,
                3488739,
                7336995,
                1882533,
                9267180,
                6976216,
                694494,
                12834555,
                4676573,
                7236424,
                8416736,
                2230147,
                8976745,
                10396871,
                8945291,
                7326682,
                1997036,
                3331646,
                1904802,
                7327091,
                3482883,
                4677506,
                8218513,
                4417019,
                1159746,
                4777262,
                1442168,
                7184370,
                3474609,
                9116780,
                2733824,
                3481186,
                9159282,
                9147482,
                14896850,
                3488715,
                2776978,
                7250023,
                4778128,
                1650259,
                3377310,
                9212402,
                3468045,
                9132504,
                417905,
                7445187,
                9554331,
                7249476,
                4912028,
                9152693,
                3348892,
                9143125,
                9227442,
                6809754,
                7240592,
                8785125,
                7250826,
                5895825,
                9156822,
                11415927,
                3486516,
                3334817,
                3483134,
                3349632,
                4894521,
                4179759,
                4308574,
                1883505,
                11792977,
                794866,
                9396089,
                3337038,
                431590,
                9521162,
                3480280,
                7330786,
                6181691,
                4716100,
                3486236,
                3535784,
                14772140,
                4133852,
                3348999,
                841895,
                4744686,
                12889990,
                3486909,
                4674133,
                9371930,
                9097231,
                8895980,
                1866254,
                7501996,
                4186439,
                7216689,
                7340292,
                6775691,
                9544340,
                4908473,
                1247566,
                11477554,
                3808979,
                407021,
                2128154,
                5846368,
                8433465,
                9521554,
                5560888,
                5904027,
                9537677,
                3314496,
                5531804,
                3343019,
                4749469,
                9107248,
                7324851,
                403557,
                8943107,
                4130097,
                9530962,
                5552949,
                3646692,
                776467,
                1192726,
                7240950,
                1932033,
                8936399,
                1874853,
                8868290,
                7339891,
                9107180,
                9303818,
                7339686,
                12888861,
                7238096,
                7263512,
                3488707,
                3487744,
                9881029,
                3487818,
                5581523,
                9495183,
                8798625,
                9523011,
                8049429,
                3500626,
                7249468,
                12091001,
                14762662,
                3335959,
                4569265,
                2356365,
                4950286,
                9214551,
                3500808,
                6496459,
                3830077,
                5849442,
                8453897,
                5896289,
                7263700,
                9165697,
                7267766,
                3330259,
                7263756,
                5174124,
                9524691,
                669562,
                4780135,
                1242859,
                8416885,
                6292883,
                3494572,
                9131185,
                3565255,
                389100,
                1449847,
                721384,
                2021321,
                9165953,
                3341705,
                3330247,
                7258112,
                9846419,
                3465907,
                388274,
                6546125,
                7317337,
                3324745,
                3348934,
                2660113,
                4699001,
                9832933,
                9326263,
                9553956,
                3772993,
                3483779,
                1679972,
                652685,
                7118642,
                14742105,
                3486889,
                5387673,
                7010637,
                559495,
                4720262,
                14802543,
                3493808,
                3347635,
                459795,
                3482685,
                4780111,
                4314903,
                1960182,
                7238100,
                2788722,
                4783027,
                2716889,
                9164642,
                3339096,
                9850331,
                4940347,
                12906702,
                3349725,
                4151907,
                7268050,
                3625453,
                5590336,
                3999298,
                11419528,
                3472865,
                1938534,
                386274,
                3496489,
                3340103,
                5847891,
                11751490,
                453278,
                5899040,
                781937,
                2726006,
                3654923,
                3812529,
                3483334,
                9365255,
                3846687,
                3348950,
                2001062,
                7236316,
                5928324,
                9111324,
                5103314,
                1957766,
                4524716,
                5136774,
                9353250,
                5595126,
                8404616,
                6279348,
                2743694,
                11646241,
                4485549,
                7316396,
                7306839,
                3488943,
                12186198,
                3344325,
                3331668,
                3656250,
                4122403,
                9532000,
                2063110,
                6303775,
                3496517,
                14906292,
                3364923,
                3486357,
                11800457,
                3830462,
                5109310,
                667641,
                571362,
                662076,
                5647003,
                7339363,
                12896956,
                5586963,
                4122067,
                4673986,
                1191862,
                7238245,
                7251258,
                407693,
                7324385,
                14005558,
                2372583,
                4670039,
                2779574,
                1905199,
                4675671,
                12859658,
                7267723,
                5560280,
                2590641,
                11873822,
                2791363,
                3483287,
                6830770,
                12855119,
                12856332,
                720365,
                2749354,
                4786655,
                3314639,
                7238427,
                7336970,
                3487751,
                5200833,
                7268258,
                8804105,
                9337838,
                4754270,
                4232429,
                6971578,
                11986865,
                8395964,
                395334,
                9155129,
                3335568,
                3483275,
                3486999,
                3147570,
                4775746,
                5845525,
                6293375,
                3334076,
                9176863,
                4739281,
                7238415,
                2578419,
                9579925,
                459707,
                9123470,
                2240612,
                3486310,
                5899043,
                4760563,
                4089678,
                3480205,
                9155522,
                12840258,
                5528449,
                14885367,
                7332340,
                3483121,
                9552074,
                4785805,
                7240363,
                6050159,
                7183444,
                8468178,
                13888559,
                4754986,
                11749989,
                3302126,
                5519490,
                466990,
                450951,
                5938880,
                11987707,
                8936347,
                5122528,
                12904214,
                7254662,
                2056342,
                3471118,
                3836644,
                4727371,
                4465061,
                6624163,
                1940592,
                459188,
                1675838,
                11521274,
                7258039,
                7242303,
                622696,
                4782077,
                10604138,
                3314631,
                2895219,
                3902736,
                7186723,
                3325810,
                3378116,
                7340062,
                9577764,
                7327144,
                1867995,
                407052,
                2908704,
                2620389,
                4169249,
                2620121,
                2142224,
                10158420,
                3340292,
                9516700,
                9106683,
                9321000,
                9516432,
                6628599,
                1893332,
                1001952,
                3340366,
                12917166,
                7059793,
                3335753,
                7187719,
                2749453,
                2030956,
                404603,
                7028949,
                2115844,
                4173987,
                2731280,
                8784152,
                3483921,
                7323858,
                3501963,
                9337658,
                2556376,
                7759633,
                4096321,
                6013507,
                3485903,
                3343112,
                2250428,
                9393226,
                4044067,
                2250223,
                2794708,
                6289919,
                2671782,
                4781882,
                9414099,
                7242051,
                4146718,
                789343,
                7318902,
                7267816,
                9135711,
                6886283,
                12853304,
                6182012,
                1869172,
                5875119,
                7037724,
                9553955,
                12844452,
                3490891,
                3468007,
                457241,
                460073,
                4500647,
                399084,
                3703807,
                9165114,
                4675741,
                14904516,
                423008,
                2107546,
                9103173,
                3483357,
                3335717,
                7236265,
                3330159,
                7233821,
                3467032,
                3465551,
                9337845,
                9214577,
                3590095,
                3482605,
                447400,
                466292,
                2749896,
                9105348,
                3343009,
                4509444,
                5920214,
                2588255,
                4677090,
                11802114,
                401702,
                3702507,
                3344415,
                1223481,
                1865885,
                9342833,
                9183647,
                2230180,
                3343607,
                7334989,
                9574578,
                7198105,
                9301702,
                1722889,
                4413128,
                3751617,
                4101012,
                3336292,
                3582274,
                6568893,
                721130,
                6545131,
                10097832,
                3334874,
                7338645,
                2502980,
                9233786,
                395648,
                1859084,
                16231032,
                7340242,
                2797889,
                14798298,
                5908464,
                4928186,
                9204769,
                3341838,
                3500742,
                2802029,
                3467948,
                3480672,
                3349688,
                4146933,
                9572590,
                2247094,
                3500861,
                14755100,
                5898091,
                5130046,
                7133361,
                1891329,
                3344249,
                3499774,
                395152,
                8775622,
                2005780,
                11880815,
                1255625,
                9214518,
                4928561,
                7445402,
                14878624,
                6308107,
                2148627,
                3330237,
                9107123,
                7347268,
                431207,
                7316040,
                2742103,
                125333,
                4432262,
                4132461,
                3483360,
                14789048,
                1084317,
                12729413,
                9534077,
                9090561,
                4775637,
                3337532,
                3314627,
                1163058,
                6299081,
                9168967,
                3496503,
                10284295,
                6561186,
                7401473,
                3487887,
                3465948,
                3728935,
                2107526,
                414309,
                14789639,
                9568414,
                3337531,
                8936318,
                764991,
                9515829,
                4135700,
                4674895,
                4100395,
                3315634,
                9095641,
                2000603,
                3484828,
                5928457,
                9538257,
                3343599,
                14885303,
                4760283,
                1087682,
                1652540,
                3368133,
                8976565,
                9140815,
                6301899,
                7318437,
                3334866,
                3339803,
                759290,
                6401718,
                9337597,
                398151,
                2730672,
                7968620,
                6733230,
                3335606,
                3487037,
                11874264,
                5902288,
                5909743,
                7327030,
                9119989,
                2023149,
                7430570,
                9893454,
                7970100,
                7343280,
                1854730,
                6087090,
                9128184,
                9149608,
                2721981,
                9896349,
                7339365,
                3480254,
                4275249,
                6181933,
                14777866,
                9517958,
                3809244,
                4507960,
                1948444,
                14908652,
                6299431,
                9515242,
                2625271,
                3482884,
                3493728,
                4140392,
                9521466,
                4784362,
                455744,
                9572894,
                9256980,
                5863507,
                9337629,
                2724811,
                9355928,
                5580439,
                783584,
                3500773,
                3489576,
                9127152,
                4702844,
                8804717,
                786158,
                6797762,
                4176048,
                9133376,
                9337829,
                1372782,
                468121,
                8459194,
                6102223,
                7236379,
                3334272,
                5906244,
                9182162,
                14731090,
                3487138,
                3357367,
                689772,
                4760428,
                4746191,
                11568667,
                3340097,
                2503534,
                9186615,
                7327120,
                2814246,
                9154784,
                394368,
                9629605,
                688678,
                2580180,
                4777291,
                5529943,
                5531498,
                2590061,
                4552172,
                3479586,
                4181776,
                12900356,
                11584733,
                7243081,
                9370278,
                3471002,
                9298103,
                5793000,
                3341919,
                8466142,
                2726472,
                2784410,
                4927025,
                6285577,
                6546328,
                4707291,
                3465916,
                4336186,
                1891615,
                5520910,
                2909352,
                2653266,
                9518241,
                12725719,
                4096918,
                7238456,
                3619374,
                460887,
                3349062,
                7316241,
                4710326,
                3496522,
                2327311,
                14899173,
                6307574,
                11874449,
                6569263,
                3354404,
                1446239,
                5549431,
                2601826,
                2785536,
                7114164,
                2883712,
                4147121,
                3482404,
                3349597,
                9544939,
                3493333,
                8848381,
                9172021,
                9552450,
                5897203,
                3786140,
                898910,
                7241349,
                2573556,
                12896533,
                3874075,
                2130382,
                9152367,
                14910682,
                2909994,
                7307761,
                412493,
                8249003,
                3500702,
                12729408,
                1166710
            };
            #endregion

            var updatedKeys = new HashSet<int>();

            updatedKeys.Add(3355440);
            updatedKeys.Add(3378835);

            for (int i = 0; i < 1500; i++)
                updatedKeys.Add(i);
            

            var trueBytes = BitConverter.GetBytes(true);
            foreach (var testKey in updatedKeys)
            {
                _qdbm.Put(new QdbmKey(testKey), trueBytes);                
            }

            foreach (var testKey in updatedKeys)
            {
                var val = _qdbm.Get(new QdbmKey(testKey));

                val.Should().NotBeNull("Key: " + testKey.ToString());
                BitConverter.ToBoolean(val, 0).Should().BeTrue("Key: " + testKey.ToString());
            }
        }
    }
}